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
- Updated `micro_status/dataset.py`, `micro_status/rscm_dataset.py`, and `micro_status/mesospim_dataset.py` so Hive-targeted cleanup moves create timestamp markers for the exact `/h20/trash/autodelete/...` destination path.
- Updated `check_status.py`, `micro_status/settings.py`, and `micro_status/local_settings.py` with a separate once-daily Hive trash cleanup pass, explicit `HIVE_TRASH_LOCATION` deletion guards, and per-file/per-folder/per-marker logging for Hive trash deletions.
- Validation: `python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status` succeeded.
