# Repository Guidelines

## Project Structure & Module Organization
- Source code: `src/databricks_mcp/` (entrypoint: `__main__.py`).
- Capabilities: `src/databricks_mcp/tools/` and `src/databricks_mcp/resources/`.
- Config & utilities: `src/databricks_mcp/config.py`, `logging_config.py`, `db_client.py`.
- Tests: `tests/unit/` and `tests/integration/`.
- Docs: `docs/`.

## Build, Test, and Development Commands
- Install deps: `poetry install` (Python >=3.10,<3.13).
- Shell into venv: `poetry shell`.
- Run server (stdio): `poetry run python -m src.databricks_mcp`.
- Tests: `poetry run pytest` (quiet/report flags via `pyproject.toml`).
- Lint: `poetry run ruff check .`.
- Format: `poetry run ruff format .`.
- Docker (optional): `docker build -t databricks-mcp .` then `docker run --env-file .env databricks-mcp`.

## Coding Style & Naming Conventions
- Formatter/linter: Ruff (see `pyproject.toml`). Use `ruff format` and `ruff check`.
- Line length target: 100 (E501 ignored; rely on formatter).
- Imports: isort via Ruff, single-line (`force-single-line = true`).
- Naming: modules/files `snake_case.py`; functions/vars `snake_case`; classes `PascalCase`.
- Keep first‑party imports under `databricks_mcp` grouped as configured.

## Testing Guidelines
- Framework: Pytest.
- Location: `tests/unit/` and `tests/integration/`.
- Naming: files `test_*.py`, tests `def test_*`.
- Mocks/fixtures encouraged (see existing tests under `tests/unit/**`).
- Run selectively: `pytest tests/unit/tools/test_files_tools.py -q`.

## Commit & Pull Request Guidelines
- Commits: short, imperative subject line (e.g., "Add workspace file list tool").
- Scope in subject is helpful (e.g., `resources:` or `tools:`) when relevant.
- PRs must include: clear description, linked issue (if any), test coverage for new logic, and docs updates (`README.md`/`docs/`/capabilities) when adding tools/resources.
- Avoid unrelated refactors in feature/bugfix PRs.

## Security & Configuration Tips
- Configure Databricks auth via `.env` (copy from `.env.example`) or standard SDK methods: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.
- The `databricks:secrets:get_secret` tool is disabled by default; enable only when necessary via env (`ENABLE_GET_SECRET=true`) and never commit secrets.
- Prefer `poetry run ...` to ensure the correct environment.

## Adding Capabilities
- Resources: add under `src/databricks_mcp/resources/`.
- Tools: add under `src/databricks_mcp/tools/`.
- Import/register modules in `src/databricks_mcp/server.py` and add tests.
