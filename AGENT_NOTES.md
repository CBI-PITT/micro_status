# AGENT_NOTES.md

2026-06-09
- Created `AGENT_NOTES.md` as the single running notes file for agent-made repository changes.
- Updated `AGENTS.md` to instruct future agents to read the last 100 lines of `AGENT_NOTES.md` before making changes.
- Updated `AGENTS.md` to require appending brief dated notes with files changed, intent, and validation or follow-up.
- Updated `check_status.py` to run FastStore trash cleanup once per day for both `RSCM` and `MesoSPIM`, based on trash marker age rather than source dataset mtime.
- Updated `micro_status/settings.py` with shared FastStore trash cleanup constants, 3-day retention, and trash marker suffix.
- Updated `micro_status/dataset.py`, `micro_status/rscm_dataset.py`, and `micro_status/mesospim_dataset.py` to touch trash timestamp markers whenever datasets or cleanup artifacts are moved into FastStore trash.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.

2026-06-10
- Updated `check_status.py` to run a once-daily stale dataset move check that submits existing move jobs for any DB record older than 30 days still recorded under `/CBI_FastStore/Acquire`.
- Updated `micro_status/settings.py` with the stale-move daily marker path and age threshold; completion messaging continues to come from the existing move-complete flow.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.
- Updated `check_status.py` `check_moving()` to mark missing FastStore datasets as moved only when a Hive destination is verified, otherwise clear `moving` and pause them for follow-up.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.
- Updated `check_status.py` to emergency-purge contents of `/CBI_FastStore/trash` once per entry into the critical FastStore space threshold, while preserving the trash root and verifying each deleted path stays under that prefix.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.
- Updated `micro_status/settings.py` and `micro_status/local_settings.py` to move FastStore and Hive trash roots under `trash/autodelete`, preserving derived `RSCM` and `MesoSPIM` subpaths and existing cleanup behavior.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.

2026-09-11
- Added `instrument_id` tracking for MesoSPIM datasets: `micro_status/mesospim_dataset.py` now parses `[instrument_id]` from the `[MICROSCOPE PARAMETERS]` section of metadata files and writes it to the DB in `_specific_setup` (new datasets only).
- Added a commented one-time `ALTER TABLE dataset ADD COLUMN instrument_id TEXT` block to `create_db.py` (must be uncommented and run once against `/CBI_FastStore/Iana/RSCM_MesoSPIM_datasets.db` before the DB write works).
- Updated `micro_status/dataset.py` to add a `self.instrument_id = None` default in `Dataset.__init__` and append `Microscope: <instrument_id>` to Slack imaging messages (`imaging_started`, `imaging_finished`, `imaging_paused`) when set; RSCM and other messages unchanged.
- Parser sanity-checked against a live MesoSPIM metadata file (returns `mesoSPIM 1`).
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.
- Follow-up: run the `ALTER TABLE` migration once; existing DB rows will not be backfilled (per decision).

2026-09-15
- Added MesoSPIM usage posting to the online scheduler (scheduler.cbi.pitt.edu/cbi_logger.php), mirroring the timeLogs repo payload format.
- New module `micro_status/scheduler_report.py`: parses `[Started taking images]`/`[Stopped taking images]` (`%Y%m%d-%H%M%S`) from tile metadata files (min/max across files), builds a timeLogs-style payload (`RECORD_ID` = start `%Y%m%d%H%M%S` + zero-padded db id, `LABUSER` = path user, `MACHINENAME` = instrument_id, `START`/`END` = `YYYYMMDD-<30-min slot>` with same-slot push), POSTs with basic auth, writes timeLogs-style transmit logs, emails on failure (once per dataset via `scheduler_notified`).
- Ran one-time DB migration against `/CBI_FastStore/Iana/RSCM_MesoSPIM_datasets.db`: added `imaging_start`, `imaging_end`, `scheduler_posted`, `scheduler_record_id`, `scheduler_notified` (dataset columns 37-41); commented "run once" block added to `create_db.py` matching existing convention.
- Updated `micro_status/dataset.py` `__init__` to read the new columns positionally (37-41).
- Updated `check_status.py` `check_mesoSPIM_imaging()`: posts usage when `imaging_status == "finished"` and `scheduler_posted == 0` (btf + zarr loops); retries every scan until POST succeeds; demo/test datasets skipped.
- Updated `micro_status/settings.py`: `SCHEDULER_URL`, `SCHEDULER_LOG_DIR` (`/CBI_FastStore/Iana/scheduler_transmit_logs`), `SCHEDULER_POSTING_ENABLED = False` (flip on after validation), `SCHEDULER_POST_TEST_FAIL` spoof flag.
- Appended scheduler/email placeholder keys to `.env` (gitignored): `SCHEDULER_USER`, `SCHEDULER_PASS`, `SCHEDULER_EMAIL_SENDER`, `SCHEDULER_EMAIL_PASSWORD`, `SCHEDULER_EMAIL_RECIPIENTS`. NOTE: real credentials are NOT in the timeLogs repo (only empty `settings_TEMPLATE.ini`); user must fill `SCHEDULER_USER`/`SCHEDULER_PASS`/`SCHEDULER_EMAIL_PASSWORD` before enabling.
- Validation: `python3 -m compileall ...` succeeded; read-only test against a live metadata folder (`delima-s/5CL18/091426_2`, 40 metas) parsed start 2026-09-14 14:39:12 / end 19:02:56; payload, same-slot push, cross-midnight, disabled path and spoof-fail transmit-log/email-guard path verified with sandboxed log dir (no real POST, no DB writes).
- Follow-ups: fill `.env` credentials; set `SCHEDULER_POSTING_ENABLED = True`; confirm that path usernames and instrument_id resolve in the scheduler.

2026-09-15 (later)
- First real POST attempt failed: scheduler replied "Required field MACHINENAME has invalid value" (MACHINENAME lookup does not resolve MesoSPIM instruments). Jesse updated `cbi_logger.php` to accept a numeric `MACHINE_ID` key: mesoSPIM 1 -> 1101, mesoSPIM 2 -> 1102, mesoSPIM 3 -> 1113 (per Jesse; 1113 is not a typo).
- Added `SCHEDULER_MACHINE_IDS` mapping dict to `micro_status/settings.py`.
- Updated `micro_status/scheduler_report.py`: new `machine_id_for()` helper (normalizes instrument_id to lowercase alphanumerics, so `mesoSPIM 1`/`mesospim1` both resolve); payload now includes `MACHINE_ID` after `MACHINENAME` (MACHINENAME kept for self-description).
- Unified failure alerting per user request (Slack once per dataset for any failed-post reason): new `alert_post_failure()` sends `dataset.send_message('scheduler_post_failed')` (reuses existing Slack plumbing) and the failure email only when a POST was attempted, then sets `scheduler_notified = 1` (no new DB column). Covers unmapped/missing instrument_id, missing pi, missing metadata times after imaging finished, and transmit failures. Global conditions (posting disabled, demo datasets, missing SCHEDULER_USER/PASS) remain log-only.
- Added `scheduler_post_failed` msg_type to `send_message` msg_map in `micro_status/dataset.py` (`*WARNING: Usage of {} {} {} could NOT be posted to the scheduler*`).
- Failure email `TEST:` subject prefix now derives from `SCHEDULER_POST_TEST_FAIL` instead of a parameter.
- Validation: `python3 -m compileall ...` succeeded; sandbox test verified mapping/normalization, `MACHINE_ID: 1101` in payload, Slack-alert-once behavior, spoof-fail transmit log + email guard (no real POST). Sandbox payload for the real failed dataset (`delima-s`, START 20260915-24 / END 20260915-28) matches the rejected record, so its pending retry will now include `MACHINE_ID`.
- Observation (possible question for Jesse): the scheduler parses the numeric 20-digit `RECORD_ID` as a PHP float (echo `2.0260915121939e+19`, derived `BOT_RECORD_ID` differs in last digits). Left as-is per decision since timeLogs behaves the same.

2026-09-21
- Added RSCM usage posting to the online scheduler, mirroring the MesoSPIM flow. All 3 RSCM (ribbon scanner) scopes share one scheduler entity "Caliber Harley" (MACHINE_ID=1056) because metadata cannot distinguish them: `vs_series.dat` `<device>` is `VS4510ML` for every dataset (verified on live data).
- `micro_status/settings.py`: added `"Caliber Harley": 1056` to `SCHEDULER_MACHINE_IDS` and new `RSCM_SCHEDULER_MACHINE_NAME = "Caliber Harley"` constant. Reuses `SCHEDULER_POSTING_ENABLED` (True), so RSCM posting is live on deploy.
- `micro_status/scheduler_report.py`: new `get_rscm_imaging_times()` derives START/END from min/max mtime of ribbon tiffs under `<layer>/<color>/images/` (RSCM metadata has no stop time; per-layer `vs_slm_series.dat` files are written up-front so their mtimes are useless; walk prunes non-layer dirs and only counts tiffs in `images/` dirs so `composites_RSCM_v0.1/` cannot shift the span). New `post_rscm_usage()` mirrors `post_mesospim_usage()`: same log_once/demo/creds guards, missing pi or missing ribbon tiffs -> alert (Slack + log, no email since no POST attempted), sets `dataset.instrument_id = RSCM_SCHEDULER_MACHINE_NAME` instance-only so payload self-describes `MACHINENAME="Caliber Harley"` + `MACHINE_ID=1056`, reuses `build_usage_payload`/`SCHEDULER_POST_TEST_FAIL` spoof, success -> transmit log + DB (`imaging_start/end`, `scheduler_posted=1`, `scheduler_record_id`), failure/exception -> transmit log ERROR + `alert_post_failure(email=True)` = log.error + failure email + Slack (once per dataset via `scheduler_notified`). Failure email subject/body now parameterized by modality via `POST_MODALITY_LABELS` (`dataset.modality` -> "ERROR: RSCM Scheduler Post Failed" / MesoSPIM subject unchanged).
- `check_status.py`: imported `post_rscm_usage`; `check_RSCM_imaging()` now calls it for any discovered dataset with `imaging_status == "finished"` and `scheduler_posted == 0` (retries every scan; just-finished datasets post on the next scan, same as MesoSPIM).
- No DB migration needed (scheduler columns 37-41 already exist for all datasets); no `.env` changes (shared SCHEDULER_*/SCHEDULER_EMAIL_* credentials).
- Validation: `python3 -m compileall ...` succeeded (one transient shared-FS EIO on `__pycache__` rename, clean on retry). Sandbox test (stubbed `update_dataset_record`/`send_message`/smtplib/`requests`, sandboxed `SCHEDULER_LOG_DIR`; no real POST/DB/Slack/email): 26/26 checks passed - machine_id mapping, live read-only walk of dataset 915 (start 2026-09-10 14:33:39 = first ribbon tiff, end 2026-09-13 02:39:13 = last ribbon tiff; naive max mtime 18:11:25 was a composite), payload MACHINENAME/MACHINE_ID/RECORD_ID/START/END slots, same-slot push, cross-midnight, disabled/demo/missing-pi/missing-tiffs guards, spoof-fail + exception paths (transmit log ERROR, email subject `TEST: ERROR: RSCM Scheduler Post Failed`, Slack once).
- Deploy note: first real POST fires on the next scan for the 1 pending backfill dataset (id 915, `8257_try2_stack1`); moved/old finished datasets are not discovered by the `os.walk`, so no backfill spam. If the scheduler rejects `MACHINENAME="Caliber Harley"`, the failure path (log + email + Slack, once) fires and retries continue next scan.
