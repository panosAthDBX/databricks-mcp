# Databricks MCP Server Implementation Plan

This document outlines the tasks required to implement the Databricks MCP Server, based on the [Product Requirements Document (PRD)](databricks_mcp_prd.md) and the [Technical Architecture Document](databricks_mcp_tech_arch.md). It is intended for the development team.

## Phase 1: Project Setup & Core Infrastructure

*   [X] **Task 1.1: Initialize Project Structure:** Done
    *   Created directories: `src/databricks_mcp`, `tests`, subdirectories (`tools`, `resources`, `prompts`, `fixtures`, `unit`, `integration`).
    *   Initialized Git, created basic `README.md`, standard Python `.gitignore`, and necessary `__init__.py` files.
*   [X] **Task 1.2: Setup Dependency Management:** Done
    *   Created `pyproject.toml` with project metadata, core dependencies (`modelcontextprotocol`, `databricks-sdk`, `python-dotenv`), dev dependencies (`pytest`, `pytest-mock`, `ruff`), and Ruff configuration.
    *   Dependencies can now be installed (e.g., using `uv sync` or `pip install -e ".[dev]"`).
*   [X] **Task 1.3: Implement Configuration Loading (`src/databricks_mcp/config.py`):** Done
    *   Created `src/databricks_mcp/config.py` using Pydantic to define `DatabricksConfig`.
    *   Implemented `load_config()` function to load `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from environment variables (using `python-dotenv`). Added basic validation and error handling. Created `.env.example`.
*   [X] **Task 1.4: Implement Basic Authentication (`src/databricks_mcp/auth.py`):** Done
    *   Created `src/databricks_mcp/auth.py` with `get_authenticated_client` function.
    *   Uses loaded `DatabricksConfig` to initialize `databricks.sdk.WorkspaceClient` with host and token (PAT authentication - REQ-AUTH-01).
    *   Includes a check (`client.current_user.me()`) to verify successful authentication.
*   [X] **Task 1.5: Implement Databricks Client Wrapper (`src/databricks_mcp/databricks_client.py`):** Done
    *   Created `src/databricks_mcp/databricks_client.py` with class `DatabricksAPIClient`.
    *   The class takes an authenticated `WorkspaceClient` during initialization.
    *   Added placeholder methods corresponding to required PRD features, grouped by area (Workspace, Compute, Data, AI/ML, Jobs). Added basic logging and a `handle_databricks_errors` decorator structure.
*   [X] **Task 1.6: Setup Basic MCP Server (`src/databricks_mcp/server.py`):** Done
    *   Created `src/databricks_mcp/server.py` as the main entry point.
    *   Sets up logging, loads config (`config.py`), authenticates (`auth.py`), instantiates `DatabricksAPIClient` (`databricks_client.py`).
    *   Initializes `FastMCP` and adds the `DatabricksAPIClient` to its context.
    *   Imports placeholder tool/resource modules for future discovery. Includes `main()` function using `run_stdio_server`.
*   [X] **Task 1.7: Setup Basic Testing (`tests/`):** Done
    *   Created `tests/unit/test_config.py` with tests for loading valid/invalid configs and `.env` loading.
    *   Created `tests/unit/test_auth.py` with tests for successful authentication, auth failures, and invalid config handling, using `pytest` and `unittest.mock`.
    *   Fixed initial test failures related to Pydantic validation and imports. All tests pass.

## Phase 2: Implement MCP Resources

Implement read-only operations defined as Resources in the PRD (Section 4.2).

*   [ ] **Task 2.1: Workspace Resources (`src/databricks_mcp/resources/workspace.py`):**
    *   Implement `list_workspace_items` (REQ-WS-RES-01) using the `databricks_client`.
    *   Implement `get_notebook_content` (REQ-WS-RES-02).
    *   Implement `list_repos` (REQ-WS-RES-03).
    *   Implement `get_repo_status` (REQ-WS-RES-04).
    *   Add unit tests for these resources.
*   [ ] **Task 2.2: Compute Resources (`src/databricks_mcp/resources/compute.py`):**
    *   Implement `list_clusters` (REQ-COMP-RES-01).
    *   Implement `get_cluster_details` (REQ-COMP-RES-02).
    *   Add unit tests.
*   [ ] **Task 2.3: Data & SQL Resources (`src/databricks_mcp/resources/data.py`):**
    *   Implement `list_catalogs` (REQ-DATA-RES-01).
    *   Implement `list_schemas` (REQ-DATA-RES-02).
    *   Implement `list_tables` (REQ-DATA-RES-03), handling pagination.
    *   Implement `get_table_schema` (REQ-DATA-RES-04).
    *   Implement `preview_table` (REQ-DATA-RES-05).
    *   Add unit tests.
*   [ ] **Task 2.4: AI/ML Resources (`src/databricks_mcp/resources/ml.py`):**
    *   Implement `list_mlflow_experiments` (REQ-ML-RES-01).
    *   Implement `list_mlflow_runs` (REQ-ML-RES-02), handling pagination.
    *   Implement `get_mlflow_run_details` (REQ-ML-RES-03).
    *   Implement `list_registered_models`