from datetime import datetime
import requests
from .settings import ALLOW_MOVES_ANYTIME, RESTRICT_MOVING_TIME, MOVE_TIMES

import json
import os
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def can_be_moved():
    if ALLOW_MOVES_ANYTIME:
        return True
    # time restrictions
    if not RESTRICT_MOVING_TIME:
        return True
    # r = requests.get("http://worldtimeapi.org/api/timezone/America/New_York")
    # r_json = r.json()
    # local_time_str = r_json['datetime']
    # week_day = r_json['day_of_week']
    # print("local_time_str", local_time_str)

    week_day = datetime.today().weekday()
    if week_day == 5 or week_day == 6:
        return True

    import pytz
    # Get the current time in UTC
    current_time_utc = datetime.now(pytz.utc)
    # Get the local timezone
    local_timezone = pytz.timezone('America/New_York')
    # Convert UTC time to local time
    local_time = current_time_utc.astimezone(local_timezone)
    if local_time.hour >= MOVE_TIMES['start'] or local_time.hour <= MOVE_TIMES['stop']:
        return True
    return False


def is_night_time():
    """Check if the current time is between processing_start_time and processing_end_time."""
    from datetime import datetime, time

    now = datetime.now().time()
    start_time = time(MOVE_TIMES['start'], 0)
    end_time = time(MOVE_TIMES['stop'], 0)

    # Check if the current time is within the range
    if start_time <= now or now < end_time:
        return True
    return False


# Post a message to a channel
def post_message(channel, text):
    url = "https://slack.com/api/chat.postMessage"
    payload = {
        "channel": channel,
        "text": text,
    }
    response = requests.post(url, json=payload, headers=HEADERS)
    data = response.json()
    if data.get("ok"):
        print("Message posted successfully!")
        return data["ts"]  # Return the timestamp of the message
    else:
        print("Failed to post message:", data.get("error"))
        return None


# Fetch replies from a thread in Slack
def fetch_thread_replies(channel, thread_ts):
    """
    usage:
    BOT_TOKEN = "xoxb-your-bot-token"
    HEADERS = {
        "Authorization": f"Bearer {BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    # Specify the channel ID where the bot will post
    channel_id = "C12345678"

    # Post a new message
    thread_ts = post_message(channel_id, "Hello! This is a test message.")

    if thread_ts:
        # Simulate fetching and responding to replies
        print("Waiting for replies...")

        import time
        time.sleep(10)  # Simulate waiting for replies

        replies = fetch_thread_replies(channel_id, thread_ts)
        for reply in replies:
            user = reply.get("user")
            text = reply.get("text")
            ts = reply.get("ts")
            if user and text:
                print(f"Reply from {user}: {text}")
    """
    url = "https://slack.com/api/conversations.replies"
    params = {
        "channel": channel,
        "ts": thread_ts,
    }
    response = requests.get(url, params=params, headers=HEADERS)
    data = response.json()
    if data.get("ok"):
        return data["messages"]  # List of messages in the thread
    else:
        print("Failed to fetch replies:", data.get("error"))
        return []




ZARR_V2_METADATA_FILENAMES = {".zattrs", ".zgroup", ".zarray"}


def backup_zarr_v2_metadata_to_zip(
    ome_zarr_path,
    *,
    include_manifest=True,
    compression=ZIP_DEFLATED,
):
    """
    Create a ZIP backup of Zarr v2 metadata files from an .ome.zarr dataset.

    Captures:
      - .zattrs and .zgroup (anywhere under the root)
      - .zarray (for each array under the root; typical for multiscale pyramids)

    Preserves folder structure by storing files with paths relative to ome_zarr_path.

    Parameters
    ----------
    ome_zarr_path:
        Filesystem path to the dataset root folder (e.g. ".../something.ome.zarr").
    # zip_path:
    #     Output ZIP file path.
    include_manifest:
        If True, writes MANIFEST.json listing all archived files.
    compression:
        zipfile compression mode (default ZIP_DEFLATED).

    Returns
    -------
    Path to the created ZIP.
    """
    root = Path(ome_zarr_path).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"ome_zarr_path does not exist or is not a directory: {root}")

    zip_path = root / "metadata_backup.zip"
    out_zip = Path(zip_path).expanduser().resolve()
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    # Collect metadata files under root
    metadata_files = []
    for p in root.rglob("*"):
        if p.is_file() and p.name in ZARR_V2_METADATA_FILENAMES:
            metadata_files.append(p)

    # (Optional) sanity check: warn if it looks like Zarr v3 (zarr.json),
    # but we won't error; user asked for v2 metadata.
    zarr_json = root / "zarr.json"
    if zarr_json.exists():
        # Zarr v3 marker exists at root; still proceed collecting v2 files found.
        pass

    # Write the ZIP with relative paths
    archived_relpaths = []
    with ZipFile(out_zip, mode="w", compression=compression) as zf:
        for file_path in sorted(metadata_files):
            rel = file_path.relative_to(root).as_posix()
            zf.write(file_path, arcname=rel)
            archived_relpaths.append(rel)

        if include_manifest:
            manifest = {
                "root": root.name,
                "metadata_filenames": sorted(ZARR_V2_METADATA_FILENAMES),
                "file_count": len(archived_relpaths),
                "files": archived_relpaths,
            }
            zf.writestr("MANIFEST.json", json.dumps(manifest, indent=2))

    return out_zip
