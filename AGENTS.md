# AGENTS.md
## Purpose
This repository is a small Python monitoring service for RSCM and MesoSPIM datasets.
It watches acquisition and processing directories, updates a SQLite database, and posts Slack notifications.
Most code is script-driven and tightly coupled to the lab filesystem and cluster environment.
This file tells coding agents what is actually true in this repo.
Do not assume modern packaging, lint, test, or CI tooling exists unless you add it intentionally.

## Repo Facts
- Main entrypoint: `check_status.py`
- Package directory: `micro_status/`
- Other scripts: `create_db.py`, `populate_db.py`, `cleanup_db.py`
- Shell helpers: `run_cbpy.sh`, `run_rscm_cluster.sh`
- Dependency file: `requirements.txt`
- README usage is operational and environment-specific
- No `AGENTS.md` existed here before this one
- No Cursor rules were found in `.cursor/rules/` or `.cursorrules`
- No Copilot instructions were found in `.github/copilot-instructions.md`

## Environment Notes
- The code assumes lab-specific absolute paths under `/CBI_FastStore`, `/h20`, and `/h20/home/lab`
- The code expects a `.env` file for Slack credentials
- The code performs real filesystem, database, HTTP, and subprocess side effects
- The shell in this workspace uses `python3`, not `python`
- Many scripts are not portable outside the lab environment

## Setup
Run from the repository root.
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
There is no package build system, Makefile, Dockerfile, or wheel build.
Treat build validation as dependency installation plus syntax checking.
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
Safe-ish CLI inspection:
```bash
python3 micro_status/validate_tiles.py --help
```
Be careful with `python3 check_status.py`; it can touch the real database, filesystem, Slack, and cluster jobs.

## Lint
There is no committed linter configuration in this repo.
No `ruff`, `flake8`, `pylint`, `black`, `isort`, or `mypy` config files were found.
Use syntax validation as the minimum non-invasive lint check:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
```

## Tests
There is no committed automated test suite.
No `tests/` directory, `pytest.ini`, or test modules were found.
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

## Style Overview
Follow the existing Python style where practical, but prefer the smallest correct improvement.
Do not do broad cleanup or modernization unless the task requires it.

## Imports
- Group imports as stdlib, third-party, then local imports
- Prefer one import per line unless names are tightly related
- Prefer explicit imports over wildcard imports in new code
- Existing files use `from micro_status.settings import *`; do not spread that pattern further
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
- Prefer descriptive domain names like `path_on_fast_store` over vague abbreviations

## Error Handling
- Assume external failures can happen: filesystem, SQLite, HTTP, Slack, and cluster commands
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
- Prefer dry inspection and targeted validation before running operational scripts

## Working Rules
- Make the smallest correct change
- Preserve behavior unless the task is to change behavior
- Prefer focused fixes over broad refactors
- If you touch wildcard imports, hard-coded paths, or broad exception handling, improve only the area required for the task
- If you add tests, keep them isolated from real lab infrastructure and document how to run one test

## Do Not Assume
- Do not assume CI exists
- Do not assume `pytest` is installed
- Do not assume formatter or linter config exists
- Do not assume scripts are safe outside the lab environment
- Do not assume path constants can be changed casually; they are part of deployment
