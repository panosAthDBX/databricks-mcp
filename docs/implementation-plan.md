# Implementation Plan: Databricks MCP Server

This plan outlines the steps to implement the Databricks MCP Server based on the [Product Requirements Document](databricks_mcp_prd.md) and [Technical Architecture](databricks_mcp_tech_arch.md).

**Legend:**
- [ ] To Do
- [x] Done
- Requirement IDs (e.g., `REQ-AUTH-01`) refer to the PRD.

---

## Phase 1: Foundation Setup

- [x] **1.1. Initialize Project:**
    - [x] Use `Poetry` to initialize the Python project (`poetry init`).
    - [x] Configure `pyproject.toml` with basic metadata, Python version constraint (>=3.10), and initial dependencies (`mcp`, `databricks-sdk`, `pydantic-settings`, `python-dotenv`, `structlog`, `ruff`, `pytest`).
    - [x] Run `poetry lock && poetry install`.
    - **Summary:** Poetry project initialized, `pyproject.toml` created, `poetry.lock` generated, and core dependencies installed.

- [x] **1.2. Basic Project Structure:**
    - [x] Create the directory structure outlined in the Tech Arch ( `src/databricks_mcp`, `src/databricks_mcp/tools`, `src/databricks_mcp/resources`, `tests/`, etc.).
    - [x] Add `__init__.py` files where necessary.
    - **Summary:** Directory structure created as per technical architecture document. Empty `__init__.py` files added.

- [x] **1.3. Configuration Setup:**
    - [x] Implement configuration loading using `pydantic-settings` in `src/databricks_mcp/config.py`.
    - [x] Define settings for `LOG_LEVEL`, `ENABLE_GET_SECRET`. Relies on SDK defaults for Databricks Host/Auth.
    - [x] Create `.env.example` with explanations for environment variables.
    - **Summary:** Configuration model created in `config.py` using pydantic-settings. `.env.example` file added.

- [x] **1.4. Logging Setup:**
    - [x] Implement structured JSON logging configuration in `src/databricks_mcp/logging_config.py` using `structlog`.
    - [x] Configure logger based on `LOG_LEVEL` from `config.py`.
    - [x] Include timestamp, level, message, logger name.
    - **Summary:** Structured logging configured in `logging_config.py`.

- [x] **1.5. Databricks Client Wrapper & Authentication:**
    - [x] Create `src/databricks_mcp/db_client.py`.
    - [x] Implement `get_db_client()` to initialize and return a singleton `databricks.sdk.WorkspaceClient`.
    - [x] Rely on the SDK's built-in authentication mechanisms.
    - [x] Perform initial connection check (`current_user.me()`).
    - **Summary:** Wrapper for `databricks-sdk` `WorkspaceClient` created in `db_client.py`, handling initialization and authentication.

- [x] **1.6. Error Mapping Setup:**
    - [x] Create `src/databricks_mcp/error_mapping.py`.
    - [x] Define `map_databricks_errors` decorator to wrap SDK calls.
    - [x] Implement mapping for common SDK exceptions (`NotFound`, `PermissionDenied`, `BadRequest`, etc.) to standard `mcp.errors.ErrorCode` and raise `mcp.errors.MCPError`.
    - **Summary:** Error mapping decorator created in `error_mapping.py` to translate SDK exceptions to MCP errors.

- [x] **1.7. Basic Server Setup:**
    - [x] Create `src/databricks_mcp/server.py` to initialize `FastMCP`.
    - [x] Import necessary components (config, logger, db_client).
    - [x] Perform initial DB client connection check on startup.
    - [x] Create `src/databricks_mcp/__main__.py` to setup logging and run the `FastMCP` instance.
    - **Summary:** Basic `FastMCP` server initialized in `server.py`, runnable via `__main__.py`. Logging and initial DB check integrated.

---

## Phase 2: Compute Capabilities

- [x] **2.1. Implement Compute Resources:**
    - [x] Create `src/databricks_mcp/resources/compute.py`.
    - [x] Implement `list_clusters` resource (`REQ-COMP-RES-01`) using `@mcp.resource()` decorator. Use `db_client` and apply error mapping.
    - [x] Implement `get_cluster_details` resource (`REQ-COMP-RES-02`). Use `db_client` and apply error mapping.
    - [x] Import and register these resources in `server.py`.
    - **Summary:** Compute resources (`list_clusters`, `get_cluster_details`) implemented in `resources/compute.py` and registered in `server.py`.

- [x] **2.2. Implement Compute Tools:**
    - [x] Create `src/databricks_mcp/tools/compute.py`.
    - [x] Implement `start_cluster` tool (`REQ-COMP-TOOL-01`) using `@mcp.tool()` decorator. Use `db_client` and apply error mapping.
    - [x] Implement `terminate_cluster` tool (`REQ-COMP-TOOL-02`). Use `db_client` and apply error mapping.
    - [x] Import and register these tools in `server.py`.
    - **Summary:** Compute tools (`start_cluster`, `terminate_cluster`) implemented in `tools/compute.py` and registered in `server.py`.

- [x] **2.3. Unit Tests for Compute:**
    - [x] Create corresponding test files in `tests/unit/resources/` and `tests/unit/tools/`.
    - [x] Write unit tests mocking the `db_client` calls and verifying correct resource/tool definition, parameter handling, return values, and error mapping for Compute capabilities.
    - **Summary:** Unit tests added for Compute tools and resources in `tests/unit/`, mocking `db_client` calls.

---

## Phase 3: Workspace & Repo Capabilities

- [x] **3.1. Implement Workspace/Repo Resources:**
    - [x] Create `src/databricks_mcp/resources/workspace.py`.
    - [x] Implement `list_workspace_items` (`REQ-WS-RES-01`), `get_notebook_content` (`REQ-WS-RES-02`), `list_repos` (`REQ-WS-RES-03`), `get_repo_status` (`REQ-WS-RES-04`).
    - [x] Use `@mcp.resource()`, `db_client`, error mapping. Handle pagination if needed by SDK.
    - [x] Register in `server.py`.
    - **Summary:** Workspace/Repo resources (`list_workspace_items`, `get_notebook_content`, `list_repos`, `get_repo_status`) implemented and registered.

- [x] **3.2. Implement Workspace Tools:**
    - [x] Create `src/databricks_mcp/tools/workspace.py`.
    - [x] Implement `run_notebook` (`REQ-WS-TOOL-01`) and `execute_code` (`REQ-WS-TOOL-02`).
    - [x] Use `@mcp.tool()`, `db_client`, error mapping.
    - [x] Address asynchronous nature: Implemented blocking behavior using SDK's `.result()` and documented this limitation.
    - [x] Register in `server.py`.
    - **Summary:** Workspace tools (`run_notebook`, `execute_code`) implemented with blocking behavior and registered.

- [x] **3.3. Unit Tests for Workspace/Repo:**
    - [x] Add unit tests in `tests/unit/` for Workspace/Repo tools and resources, mocking `db_client`.
    - [x] Verify parameters, returns, errors, and mocking of synchronous waits.
    - **Summary:** Unit tests added for Workspace/Repo capabilities.

---

## Phase 4: Data & SQL Capabilities

- [x] **4.1. Implement Data/SQL Resources:**
    - [x] Create `src/databricks_mcp/resources/data.py`.
    - [x] Implement `list_catalogs` (`REQ-DATA-RES-01`), `list_schemas` (`REQ-DATA-RES-02`), `list_tables` (`REQ-DATA-RES-03`), `get_table_schema` (`REQ-DATA-RES-04`), `preview_table` (`REQ-DATA-RES-05`), `list_sql_warehouses` (`REQ-DATA-RES-06`).
    - [x] Use `@mcp.resource()`, `db_client`, error mapping. Handle pagination.
    - [x] Register in `server.py`.
    - **Summary:** Data/SQL resources implemented and registered. `preview_table` uses synchronous execution.

- [x] **4.2. Implement Data/SQL Tools:**
    - [x] Create `src/databricks_mcp/tools/data.py`.
    - [x] Implement `execute_sql` (`REQ-DATA-TOOL-01`). **MUST** follow async pattern (return `statement_id`).
    - [x] Implement `get_statement_result` (`REQ-DATA-TOOL-02`) to poll/fetch results using `statement_id`.
    - [x] Implement `start_sql_warehouse` (`REQ-DATA-TOOL-03`), `stop_sql_warehouse` (`REQ-DATA-TOOL-04`).
    - [x] Use `@mcp.tool()`, `db_client`, error mapping.
    - [x] Register in `server.py`.
    - **Summary:** Data/SQL tools implemented, including async pattern for `execute_sql` / `get_statement_result`.

- [x] **4.3. Unit Tests for Data/SQL:**
    - [x] Add unit tests in `tests/unit/` for Data/SQL tools and resources, mocking `db_client`.
    - [x] Verify async pattern for SQL execution.
    - **Summary:** Unit tests added for Data/SQL capabilities, including async SQL execution tests.

---

## Phase 5: Job Capabilities

- [x] **5.1. Implement Job Resources:**
    - [x] Create `src/databricks_mcp/resources/jobs.py`.
    - [x] Implement `list_jobs` (`REQ-JOB-RES-01`), `get_job_details` (`REQ-JOB-RES-02`), `list_job_runs` (`REQ-JOB-RES-03`).
    - [x] Use `@mcp.resource()`, `db_client`, error mapping. Handle pagination.
    - [x] Register in `server.py`.
    - **Summary:** Job resources (`list_jobs`, `get_job_details`, `list_job_runs`) implemented and registered.

- [x] **5.2. Implement Job Tools:**
    - [x] Create `src/databricks_mcp/tools/jobs.py`.
    - [x] Implement `run_job_now` (`REQ-JOB-TOOL-01`). Address asynchronous nature (document blocking or implement polling).
    - [x] Use `@mcp.tool()`, `db_client`, error mapping.
    - [x] Register in `server.py`.
    - **Summary:** Job tool (`run_job_now`) implemented with blocking behavior and registered.

- [x] **5.3. Unit Tests for Jobs:**
    - [x] Add unit tests in `tests/unit/` for Job tools and resources, mocking `db_client`.
    - [x] Verify parameters, returns, errors, and mocking of synchronous waits.
    - **Summary:** Unit tests added for Job capabilities.

---

## Phase 6: File Management Capabilities (DBFS & Volumes)

- [x] **6.1. Implement File Resources:**
    - [x] Create `src/databricks_mcp/resources/files.py`.
    - [x] Implement `list_files` (`REQ-FILE-RES-01`). Use `db_client` (likely `w.dbfs.list` or Volume equivalents), error mapping. Handle DBFS vs Volume paths.
    - [x] Register in `server.py`.
    - **Summary:** File resource (`list_files`) implemented and registered, using DBFS API primarily.

- [x] **6.2. Implement File Tools:**
    - [x] Create `src/databricks_mcp/tools/files.py`.
    - [x] Implement `read_file` (`REQ-FILE-TOOL-01`), `write_file` (`REQ-FILE-TOOL-02`), `delete_file` (`REQ-FILE-TOOL-03`), `create_directory` (`REQ-FILE-TOOL-04`).
    - [x] Use `db_client` (DBFS/Volume APIs), `@mcp.tool()`, error mapping. Handle potential base64 encoding for binary data.
    - [x] Register in `server.py`.
    - **Summary:** File tools (`read_file`, `write_file`, `delete_file`, `create_directory`) implemented using DBFS API primarily and registered.

- [x] **6.3. Unit Tests for Files:**
    - [x] Add unit tests in `tests/unit/` for File tools and resources, mocking `db_client` (DBFS/Volume APIs).
    - **Summary:** Unit tests added for File Management capabilities.

---

## Phase 7: AI & Machine Learning Capabilities

- [x] **7.1. Implement ML Resources:**
    - [x] Create `src/databricks_mcp/resources/ml.py`.
    - [x] Implement `list_mlflow_experiments` (`REQ-ML-RES-01`), `list_mlflow_runs` (`REQ-ML-RES-02`), `get_mlflow_run_details` (`REQ-ML-RES-03`), `list_registered_models` (`REQ-ML-RES-04`), `get_model_version_details` (`REQ-ML-RES-05`).
    - [x] Use `db_client` (MLflow APIs via SDK), `@mcp.resource()`, error mapping.
    - [x] Register in `server.py`.
    - **Summary:** MLflow resources implemented and registered.

- [x] **7.2. Implement ML Tools:**
    - [x] Create `src/databricks_mcp/tools/ml.py`.
    - [x] Implement `query_model_serving_endpoint` (`REQ-ML-TOOL-01`).
    - [x] Implement Vector Search tools: `add_to_vector_index` (`REQ-ML-TOOL-02`), `query_vector_index` (`REQ-ML-TOOL-03`).
    - [x] Use `db_client`, `@mcp.tool()`, error mapping. Added checks for Vector Search SDK availability.
    - [x] Register in `server.py`.
    - **Summary:** Model Serving and Vector Search tools implemented and registered.

- [x] **7.3. Unit Tests for AI/ML:**
    - [x] Add unit tests in `tests/unit/` for ML tools and resources, mocking `db_client` (MLflow/Serving/Vector Search APIs).
    - **Summary:** Unit tests added for AI/ML capabilities.

---

## Phase 8: Secrets Management Capabilities

- [x] **8.1. Implement Secret Resources:**
    - [x] Create `src/databricks_mcp/resources/secrets.py`.
    - [x] Implement `list_secret_scopes` (`REQ-SEC-RES-01`), `list_secrets` (`REQ-SEC-RES-02`).
    - [x] Use `db_client`, `@mcp.resource()`, error mapping.
    - [x] Register in `server.py`.
    - **Summary:** Secret resources (`list_secret_scopes`, `list_secrets`) implemented and registered.

- [x] **8.2. Implement Secret Tools:**
    - [x] Create `src/databricks_mcp/tools/secrets.py`.
    - [x] Implement `get_secret` (`REQ-SEC-TOOL-01`), `put_secret` (`REQ-SEC-TOOL-02`), `delete_secret` (`REQ-SEC-TOOL-03`).
    - [x] Use `db_client`, `@mcp.tool()`, error mapping.
    - [x] **Crucially:** Implement conditional registration for `get_secret` in `server.py` based on the `ENABLE_GET_SECRET` configuration flag (`NFR-SEC-01`). Add clear warnings in logs if enabled.
    - **Summary:** Secret tools (`put_secret`, `delete_secret`, conditionally `get_secret`) implemented and registered.

- [x] **8.3. Unit Tests for Secrets:**
    - [x] Add unit tests in `tests/unit/` for Secret tools and resources, mocking `db_client`.
    - [x] Verify conditional registration logic for `get_secret`.
    - **Summary:** Unit tests added for Secrets capabilities, including conditional logic test.

---

## Phase 9: Finalization & Deployment

- [x] **9.1. Refine Prompts:**
    - [x] Create `src/databricks_mcp/prompts/example_prompt.py`.
    - [x] Implement placeholder prompt (`REQ-PROMPT-01`) using `@mcp.prompt()`.
    - [x] Add commented-out registration in `server.py`.
    - **Summary:** Example placeholder prompt added.

- [x] **9.2. Integration Testing Strategy:**
    - [x] Define approach: Manual testing against dev workspace adopted as initial strategy.
    - [ ] Execute integration tests for key workflows. (Manual step)
    - **Summary:** Integration testing strategy (manual) defined.

- [x] **9.3. Containerization:**
    - [x] Create `Dockerfile` using Poetry multi-stage build.
    - [x] Build and test the Docker image locally (assuming Docker environment is available).
    - **Summary:** Dockerfile created.

- [x] **9.4. Documentation & Cleanup:**
    - [x] Finalize `README.md` with accurate setup, configuration, and usage instructions.
    - [x] Ensure adequate docstrings for implemented tools and resources.
    - [x] Run linters (`ruff check .`, `ruff format .`) and address issues. Configured S101 ignore for tests.
    - [x] Created `docs/capabilities.md` summarizing all available tools and resources.
    - [ ] Perform final code review. (Manual step)
    - **Summary:** README finalized, docstrings reviewed, linters run and configured. Capability documentation created.

---