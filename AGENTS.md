# AGENTS.md
## Purpose
This repository is a small Python monitoring service for RSCM and MesoSPIM datasets.
It watches acquisition and processing folders, updates a SQLite database, posts Slack notifications, and submits cluster jobs.
Most code is script-driven and coupled to the lab filesystem and SLURM environment.
This file tells coding agents what is true in this repo today.
Do not assume modern packaging, CI, lint, or test tooling exists unless you add it explicitly.

## Repo Facts
- Main entrypoint: `check_status.py`
- Package directory: `micro_status/`
- Other scripts: `create_db.py`, `populate_db.py`, `cleanup_db.py`
- Shell helpers: `run_cbpy.sh`, `run_rscm_cluster.sh`
- Dependency file: `requirements.txt`
- No Cursor rules were found in `.cursor/rules/` or `.cursorrules`
- No Copilot instructions were found in `.github/copilot-instructions.md`

## Environment Notes
- Run commands from the repository root
- Use `python3`, not `python`
- The code assumes absolute lab paths under `/CBI_FastStore`, `/h20`, and `/h20/home/lab`
- `.env` is used for Slack credentials
- `micro_status/local_settings.py` exists locally and is intended for machine-specific private settings
- Many scripts have real side effects: filesystem mutation, SQLite writes, Slack messages, `sbatch`, `scancel`, `rclone`
- `python3 check_status.py` is not a safe smoke test; it can touch production-like resources

## Setup
Install dependencies:
```bash
python3 -m pip install -r requirements.txt
```
Optional virtualenv setup:
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Build
There is no package build system, wheel build, Makefile, or Docker build.
Treat build validation as syntax checking plus dependency installation.
Recommended validation command:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
```
Observed status: `python3 -m compileall ...` succeeds.

## Run
Main monitor:
```bash
python3 check_status.py
```
Database helpers:
```bash
python3 create_db.py
python3 populate_db.py
python3 cleanup_db.py
```
Safer CLI inspection:
```bash
python3 micro_status/validate_tiles.py --help
```

## Lint
There is no committed linter configuration.
No `ruff`, `flake8`, `pylint`, `black`, `isort`, or `mypy` config files were found.
Use syntax validation as the minimum non-invasive lint check:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
```

## Tests
There is no committed automated test suite.
No `tests/` directory, `pytest.ini`, `conftest.py`, or test modules were found.
Observed status:
- `python3 -m pytest --version` fails because `pytest` is not installed here
- There are no committed tests to run
Use these checks instead for small changes:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
python3 micro_status/validate_tiles.py --help
```

## Single Test Guidance
There is currently no real single-test command because there is no committed test suite.
If pytest tests are added later, use:
```bash
python3 -m pytest path/to/test_file.py::test_name
```
Until then, do not claim that a single-test workflow exists.

## Config Guidance
- Treat `micro_status/settings.py` as operational code plus defaults
- Keep secrets in `.env`
- Keep machine- or deployment-specific private values in `micro_status/local_settings.py`
- Do not commit real tokens, private path roots, or PI-specific private config to tracked files unless explicitly asked
- Be careful when editing settings: this repo currently still contains some tracked hard-coded operational values

## Style Overview
Follow the existing Python style where practical, but prefer the smallest correct improvement.
Do not do broad cleanup or modernization unless the task requires it.

## Imports
- Group imports as stdlib, third-party, then local imports
- Prefer one import per line unless names are tightly related
- Prefer explicit imports over wildcard imports in new code
- Existing files use `from micro_status.settings import *`; do not spread that pattern further unless required for consistency in the edited area
- Prefer relative imports inside `micro_status/` when editing package modules
- Avoid function-local imports unless they defer optional or heavy dependencies

## Formatting
- Use 4-space indentation
- Stay near normal PEP 8 formatting; there is no enforced formatter
- Keep blank lines between top-level functions and classes
- Keep docstrings short and practical
- Preserve shebangs on executable scripts
- Add comments only for non-obvious filesystem, cluster, or image-processing logic

## Types
- The codebase is mostly untyped
- Do not add type hints everywhere as a drive-by refactor
- Add narrow type hints only where they improve edited or new code
- Prefer built-in generics like `list[str]` and `dict[str, object]` if you add hints
- Do not introduce a type-checker config unless typing is the task

## Naming
- Functions and variables: `snake_case`
- Classes: `PascalCase`
- Constants and settings: `UPPER_SNAKE_CASE`
- Prefer descriptive domain names like `path_on_fast_store`, `processing_summary`, and `move_complete_marker`

## Error Handling
- Assume external failures can happen: filesystem, SQLite, HTTP, Slack, `rclone`, and SLURM commands
- Avoid new bare `except:` blocks
- Catch specific exceptions when feasible
- Log enough context to identify the dataset, path, job, or external call that failed
- Only swallow exceptions when the failure is expected and the code can safely continue
- Prefer explicit status changes or returns over hidden mutation after failure

## Logging
- Use `logging.getLogger(__name__)`
- Log operational events that help debug automation and external integrations
- Keep log messages specific and actionable
- Do not log secrets, tokens, or `.env` contents

## Database
- The repo uses direct `sqlite3` access with short-lived connections
- Commit explicitly after writes
- Close connections promptly
- Prefer parameterized SQL in new code even though older code often uses f-strings
- Be careful with schema assumptions; many fields are accessed by positional index
- Avoid schema changes unless the task clearly requires them

## Filesystem And Side Effects
- Expect hard-coded lab paths and existing storage conventions
- Use `Path` when it helps, but do not rewrite entire files just to replace `os.path`
- Check whether a command will touch production-like paths before running it
- Do not delete or move dataset files unless the task explicitly requires it
- Slack posting is real when `MESSAGES_ENABLED` is true
- Cluster commands such as `sbatch`, `squeue`, and `scancel` are used directly
- The move workflow now creates per-dataset SLURM scripts and uses `rclone copy`, `rclone check`, and then moves the source dataset to FastStore trash
- Prefer dry inspection and targeted validation before running operational scripts

## Working Rules
- Make the smallest correct change
- Preserve behavior unless the task is to change behavior
- Prefer focused fixes over broad refactors
- If you touch wildcard imports, hard-coded paths, or broad exception handling, improve only the area required for the task
- If you add tests later, keep them isolated from real lab infrastructure and document how to run one test

## Do Not Assume
- Do not assume CI exists
- Do not assume `pytest` is installed
- Do not assume formatter or linter config exists
- Do not assume scripts are safe outside the lab environment
- Do not assume path constants can be changed casually; they are part of deployment
