import json
import logging
import math
import os
import re
import smtplib
import sqlite3
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from .settings import *

log = logging.getLogger(__name__)

METADATA_TIME_FORMAT = "%Y%m%d-%H%M%S"
SCHEDULER_TIME_SLOT_MINS = 30

_logged_once = set()


def log_once(key, message, level=logging.INFO):
    if key in _logged_once:
        return
    _logged_once.add(key)
    log.log(level, message)


def get_imaging_times(metadata_files):
    """
    Return (start, end) datetimes parsed from the [TIMING INFORMATION] section
    of MesoSPIM tile metadata files: the earliest [Started taking images] and
    the latest [Stopped taking images] value across all files.
    Returns None while either value is missing (e.g. imaging still running).
    """
    started_times = []
    stopped_times = []
    for metadata_file in metadata_files:
        try:
            with open(metadata_file, 'r') as f:
                lines = f.readlines()
        except OSError as e:
            log.warning(f"Could not read metadata file {metadata_file}: {e}")
            continue
        for line in lines:
            timestamps = re.findall(r"\d{8}-\d{6}", line)
            if not timestamps:
                continue
            if "[Started taking images]" in line:
                started_times.append(datetime.strptime(timestamps[0], METADATA_TIME_FORMAT))
            elif "[Stopped taking images]" in line:
                stopped_times.append(datetime.strptime(timestamps[0], METADATA_TIME_FORMAT))
    if not started_times or not stopped_times:
        return None
    return min(started_times), max(stopped_times)


def time_slot(dt, slot_mins=SCHEDULER_TIME_SLOT_MINS):
    # 30-minute block index [0-47] mapping to Midnight(0) - 23:30(47)
    return str(math.floor((dt.hour * 60 + dt.minute) / slot_mins)).zfill(2)


def machine_id_for(instrument_id):
    """
    Look up the scheduler MACHINE_ID for an instrument_id parsed from
    metadata. Keys are compared with lowercase letters and digits only, so
    "mesoSPIM 1", "mesospim 1" and "mesoSPIM1" all resolve. Returns None if
    the instrument has no mapping.
    """
    if not instrument_id:
        return None
    normalized = re.sub(r"[^a-z0-9]", "", str(instrument_id).lower())
    for name, machine_id in SCHEDULER_MACHINE_IDS.items():
        if re.sub(r"[^a-z0-9]", "", str(name).lower()) == normalized:
            return machine_id
    return None


def build_usage_payload(dataset, start, end, slot_mins=SCHEDULER_TIME_SLOT_MINS):
    # Same convention as timeLogs: if start and end fall into the same slot,
    # push the end one slot forward so the usage still registers.
    if time_slot(start, slot_mins) == time_slot(end, slot_mins):
        end = end + timedelta(minutes=slot_mins)
    record_id = int(start.strftime("%Y%m%d%H%M%S") + str(dataset.db_id).zfill(6))
    return {
        "RECORD_ID": record_id,
        "LABUSER": dataset.pi,
        "MACHINENAME": dataset.instrument_id,
        "MACHINE_ID": machine_id_for(dataset.instrument_id),
        "START": start.strftime("%Y%m%d") + "-" + time_slot(start, slot_mins),
        "END": end.strftime("%Y%m%d") + "-" + time_slot(end, slot_mins),
    }


def alert_post_failure(dataset, reason, payload=None, response_text=None, email=False):
    """
    Alert once per dataset (scheduler_notified flag) that its usage could not
    be posted to the scheduler: Slack message always, failure email only when
    a POST was actually attempted and failed.
    """
    if dataset.scheduler_notified:
        return
    dataset.send_message('scheduler_post_failed')
    if email and payload is not None:
        send_failure_email(payload, response_text)
    update_dataset_record(dataset.db_id, scheduler_notified=1)
    log.error(f"Scheduler usage post failed for {dataset.path_on_fast_store}: {reason}")


def update_dataset_record(db_id, **fields):
    con = sqlite3.connect(DB_LOCATION)
    cur = con.cursor()
    assignments = ", ".join(f"{name} = ?" for name in fields)
    cur.execute(f"UPDATE dataset SET {assignments} WHERE id = ?", (*fields.values(), db_id))
    con.commit()
    con.close()


def write_transmit_log(payload, response_text, error=False):
    log_dir = os.path.join(SCHEDULER_LOG_DIR, datetime.now().strftime("%Y"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, datetime.now().strftime("%Y%m%d") + ".txt")
    timestamp = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    line_type = "ERROR: TRANSMIT FAILED" if error else "TRANSMITTED"
    with open(log_file, "a", newline="") as f:
        f.write(f"{timestamp} - {line_type}: {json.dumps(payload)}\n")
        f.write(f"{timestamp} - OUTPUT: {response_text}\n")


def send_failure_email(payload, response_text):
    sender = os.getenv("SCHEDULER_EMAIL_SENDER")
    password = os.getenv("SCHEDULER_EMAIL_PASSWORD")
    recipients = [x.strip() for x in os.getenv("SCHEDULER_EMAIL_RECIPIENTS", "").split(",") if x.strip()]
    if not sender or not password or not recipients:
        log.error("Scheduler failure email not sent: missing SCHEDULER_EMAIL_* credentials in .env")
        return
    smtp_server = os.getenv("SCHEDULER_SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SCHEDULER_SMTP_PORT", "587"))

    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    # TEST prefix when the spoof-fail setting is active
    message["Subject"] = ("TEST: " if SCHEDULER_POST_TEST_FAIL else "") + "ERROR: MesoSPIM Scheduler Post Failed"
    body = (
        "This email is being sent because posting MesoSPIM usage to the "
        "scheduler system failed.\n\n"
        f"TRANSMITTED: {json.dumps(payload)}\n\n"
        f"OUTPUT: {response_text}\n"
    )
    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, message.as_string())
        log.info(f"Sent scheduler failure email to {recipients}")
    except (smtplib.SMTPException, OSError) as e:
        log.error(f"Failed to send scheduler failure email to {recipients}: {e}")


def is_demo_dataset(dataset):
    return "demo" in str(dataset.name).lower() or "test" in str(dataset.path_on_fast_store).lower()


def post_mesospim_usage(dataset):
    """
    Build and transmit a MesoSPIM usage record to the online scheduler for a
    dataset whose imaging has finished. Safe to call on every scan: it retries
    until the POST succeeds, then marks the dataset as posted in the DB.
    """
    if not SCHEDULER_POSTING_ENABLED:
        log_once(
            f"{dataset.db_id}:disabled",
            f"Scheduler posting disabled; would transmit usage for {dataset.path_on_fast_store}: "
            f"pi={dataset.pi}, instrument_id={dataset.instrument_id}",
        )
        return

    if is_demo_dataset(dataset):
        log_once(f"{dataset.db_id}:demo", f"Ignoring scheduler usage post for demo dataset {dataset}")
        return

    machine_id = machine_id_for(dataset.instrument_id)
    if not dataset.pi or not dataset.instrument_id or machine_id is None:
        alert_post_failure(
            dataset,
            f"pi={dataset.pi}, instrument_id={dataset.instrument_id}, machine_id={machine_id}",
        )
        return

    times = get_imaging_times(dataset.metadata_files)
    if not times:
        alert_post_failure(
            dataset,
            "no [Started taking images]/[Stopped taking images] times found in metadata",
        )
        return
    start, end = times

    user = os.getenv("SCHEDULER_USER")
    password = os.getenv("SCHEDULER_PASS")
    if not user or not password:
        log_once(
            f"{dataset.db_id}:no_creds",
            "Scheduler POST skipped: missing SCHEDULER_USER/SCHEDULER_PASS in .env",
            level=logging.WARNING,
        )
        return

    payload = build_usage_payload(dataset, start, end)

    if SCHEDULER_POST_TEST_FAIL:
        # Spoof a failed transmit (like timeLogs' fail_transmit) to exercise
        # the error log and email path without contacting the scheduler.
        class FakeFailedResponse:
            ok = False
            text = "This is only a test"
        response = FakeFailedResponse()
    else:
        try:
            response = requests.post(SCHEDULER_URL, json=payload, auth=(user, password), timeout=30)
        except requests.RequestException as e:
            write_transmit_log(payload, str(e), error=True)
            alert_post_failure(dataset, str(e), payload=payload, response_text=str(e), email=True)
            return

    if response.ok:
        write_transmit_log(payload, response.text)
        update_dataset_record(
            dataset.db_id,
            imaging_start=start.strftime(DATETIME_FORMAT),
            imaging_end=end.strftime(DATETIME_FORMAT),
            scheduler_posted=1,
            scheduler_record_id=str(payload["RECORD_ID"]),
        )
        log.info(f"Posted MesoSPIM usage to scheduler for {dataset.path_on_fast_store}: {json.dumps(payload)}")
    else:
        write_transmit_log(payload, response.text, error=True)
        alert_post_failure(dataset, response.text, payload=payload, response_text=response.text, email=True)
