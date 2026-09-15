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
