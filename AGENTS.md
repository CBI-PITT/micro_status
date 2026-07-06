# AGENTS.md
## Purpose
This repository is a Python monitoring service for RSCM and MesoSPIM datasets.
It watches acquisition folders, updates a SQLite database, posts Slack messages, and submits cluster work.
Most code is operational and tied to lab infrastructure, so agent changes should be conservative and side-effect aware.

## Repository Layout
- Main entrypoint: `check_status.py`
- Package code: `micro_status/`
- Database helpers: `create_db.py`, `populate_db.py`, `cleanup_db.py`
- Shell helpers: `run_cbpy.sh`, `run_rscm_cluster.sh`
- Dependency file: `requirements.txt`
- Running change notes: `AGENT_NOTES.md`
- No committed `tests/` directory was found

## External Rule Files
- No Cursor rules were found in `.cursor/rules/`
- No `.cursorrules` file was found
- No Copilot instructions were found in `.github/copilot-instructions.md`

## Environment Facts
- Run commands from the repository root
- Use `python3`, not `python`
- The code assumes absolute lab paths under `/CBI_FastStore`, `/h20`, and `/h20/home/lab`
- `.env` is used for Slack credentials
- `micro_status/local_settings.py` exists locally and is intended for machine-specific private settings
- `check_status.py` is not a safe smoke test; it can touch production-like resources
- Many scripts can mutate files, write SQLite state, call Slack, or submit SLURM jobs

## Notes Workflow
- Before making new changes, read the last 100 lines of `AGENT_NOTES.md`
- Keep all brief change notes in `AGENT_NOTES.md`; do not create additional notes files
- Append concise dated notes as changes are made so later agents can recover context quickly
- Notes should mention files changed, intent, and any important validation or follow-up

## Setup
Install dependencies:
```bash
python3 -m pip install -r requirements.txt
```
Optional virtualenv:
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Build And Lint
There is no package build system, CI, or committed linter/formatter config.
No `ruff`, `flake8`, `pylint`, `black`, `isort`, or `mypy` config files were found.
Treat syntax validation as the minimum safe build/lint check:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
```
Observed status in this workspace: `python3 -m compileall ...` succeeds.

## Tests
There is no committed automated test suite.
No `tests/`, `pytest.ini`, `conftest.py`, or test modules were found.
Observed status in this workspace:
- `python3 -m pytest --version` fails because `pytest` is not installed
- There are no committed tests to run

## Single Test Guidance
There is currently no real single-test command because this repository has no committed test suite.
Do not claim that a single-test workflow exists today.
If tests are added later and `pytest` becomes available, use:
```bash
python3 -m pytest path/to/test_file.py::test_name
```

## Safe Validation Commands
Usually safe:
```bash
python3 -m compileall check_status.py create_db.py populate_db.py cleanup_db.py micro_status
```
Potentially safe only when optional scientific dependencies are installed:
```bash
python3 micro_status/validate_tiles.py --help
```
Observed status in this workspace:
- `python3 micro_status/validate_tiles.py --help` fails because `numpy` is missing
- `validate_tiles.py` also imports `skimage` and `tifffile`, which are not pinned in `requirements.txt`

Avoid using these as smoke tests unless explicitly requested:
- `python3 check_status.py`
- `python3 create_db.py`
- `python3 populate_db.py`
- `python3 cleanup_db.py`

## Config Guidance
- Treat `micro_status/settings.py` as operational defaults plus deployment config
- Keep secrets in `.env`
- Keep machine-specific private values in `micro_status/local_settings.py`
- Do not commit real Slack tokens, private path roots, or private lab config
- Be careful editing tracked settings; the repo still contains hard-coded operational values

## Architecture Notes
- `check_status.py` is the orchestration script
- `micro_status/dataset.py` defines the shared `Dataset` base class
- `micro_status/rscm_dataset.py` and `micro_status/mesospim_dataset.py` hold modality-specific logic
- SQLite access is direct and schema knowledge is embedded in code
- Status tracking is driven by filesystem inspection, DB updates, and external command execution

## Code Style
Follow the existing Python style where practical.
Prefer the smallest correct improvement over broad cleanup or modernization.
Preserve behavior unless the task is explicitly behavioral.

## Imports
- Group imports as stdlib, third-party, then local imports
- Prefer one import per line unless names are tightly related
- Prefer explicit imports in new code
- Existing files already use `from micro_status.settings import *`; do not spread that pattern further unless required for consistency in the edited area
- Prefer relative imports inside `micro_status/` when editing package modules
- Avoid function-local imports unless they defer optional or heavy dependencies

## Formatting
- Use 4-space indentation
- Stay close to normal PEP 8 formatting; there is no enforced formatter
- Keep blank lines between top-level functions and classes
- Keep docstrings short and practical
- Preserve shebangs on executable scripts
- Add comments only where filesystem, cluster, or image-processing behavior is non-obvious

## Types
- The codebase is mostly untyped
- Do not add broad type annotations as a drive-by refactor
- Add narrow type hints only when they materially improve edited code
- Prefer built-in generics such as `list[str]` and `dict[str, object]` if you add hints
- Do not introduce mypy or another type checker unless typing is the task

## Naming
- Functions and variables: `snake_case`
- Classes: `PascalCase`
- Constants and settings: `UPPER_SNAKE_CASE`
- Prefer descriptive domain names such as `path_on_fast_store`, `processing_summary`, and `move_complete_marker`

## Error Handling
- Assume filesystem, SQLite, Slack, HTTP, `rclone`, and SLURM failures can happen
- Avoid adding new bare `except:` blocks
- Catch specific exceptions when feasible
- Log enough context to identify the dataset, path, job, or external call that failed
- Only swallow exceptions when failure is expected and the monitor can safely continue
- Prefer explicit status changes or returns over hidden mutation after failure
- Existing code contains several broad exception handlers; improve only the area you are touching

## Logging
- Use `logging.getLogger(__name__)`
- Keep log messages specific and operationally useful
- Do not log secrets, tokens, or `.env` contents
- Preserve the existing file-based logging setup unless the task requires changing it

## Database
- The repo uses direct `sqlite3` access with short-lived connections
- Commit explicitly after writes
- Close connections promptly
- Prefer parameterized SQL in new code even though older code often uses f-strings
- Be careful with schema assumptions; many fields are accessed by positional index
- Avoid schema changes unless the task clearly requires them

## Filesystem And Side Effects
- Expect hard-coded lab paths and storage conventions
- Use `Path` when it helps, but do not rewrite entire files just to replace `os.path`
- Check whether a command will touch production-like paths before running it
- Do not delete or move dataset files unless the task explicitly requires it
- Slack posting is real when `MESSAGES_ENABLED` is true
- Cluster commands such as `sbatch`, `squeue`, and `scancel` are used directly
- The move workflow can call `rclone copy`, `rclone check`, and move source data to trash
- Prefer dry inspection and targeted validation before running operational scripts

## Dependency Notes
- `requirements.txt` pins Slack/XML-related packages and `imaris-ims-file-reader`
- Some imported runtime packages are not pinned there, including `tifffile`
- `validate_tiles.py` additionally depends on `numpy` and `scikit-image`
- Do not assume optional scientific dependencies are available unless you verify them

## Working Rules
- Make the smallest correct change
- Preserve behavior unless the task is to change behavior
- Prefer focused fixes over broad refactors
- Do not casually rewrite old SQL, wildcard imports, or path handling across the whole repo
- If you add tests later, keep them isolated from real lab infrastructure and document how to run one test
