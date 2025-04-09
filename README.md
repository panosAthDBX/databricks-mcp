# Databricks MCP Server

This project implements a Model Context Protocol (MCP) server for interacting with Databricks using the `databricks-sdk`.

## Overview

This server allows AI agents and other applications compatible with MCP to leverage Databricks functionalities, including:

*   Workspace management (notebooks, files, repos, secrets)
*   Compute management (clusters, SQL warehouses)
*   Data interaction (SQL execution via warehouses, catalog browsing)
*   AI/ML workflow management (MLflow, Model Serving, Vector Search)
*   Job execution & management

Refer to the [Product Requirements Document](docs/databricks_mcp_prd.md) for original features and the [Technical Architecture](docs/databricks_mcp_tech_arch.md) for design specifics.
For a detailed list of implemented tools and resources, see the [Capabilities Document](docs/capabilities.md).

## Setup

1.  **Prerequisites:**
    *   Python >=3.10,<3.13 (as required by the `mcp` package)
    *   `Poetry` (>=1.2, recommend latest)
    *   Access to a Databricks workspace
    *   Databricks authentication configured (e.g., via environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, or other methods supported by `databricks-sdk`). See [SDK Authentication](https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html).

2.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd databricks-mcp-server
    ```

3.  **Install dependencies:**
    ```bash
    poetry install
    ```
    This will create a virtual environment (if one doesn't exist) and install all dependencies specified in `pyproject.toml` and `poetry.lock`.

4.  **Activate virtual environment:**
    ```bash
    poetry shell
    ```

## Configuration

The server is configured primarily through environment variables. Create a `.env` file in the project root by copying `.env.example` and filling in your values for local development:

```bash
cp .env.example .env
# Now edit .env
```

**Required `.env` Variables:**

*   `DATABRICKS_HOST`: Your Databricks workspace URL (e.g., `https://dbc-XXXX.cloud.databricks.com`).
*   `DATABRICKS_TOKEN`: Your Databricks Personal Access Token (or configure another auth method recognized by the SDK).

**Optional `.env` Variables:**

*   `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to `INFO`.
*   `ENABLE_GET_SECRET`: Set to `true` to enable the `databricks:secrets:get_secret` tool. Defaults to `false`. **Use with extreme caution.**

## Usage

Make sure your virtual environment is activated (`poetry shell`) and your `.env` file is configured.

Run the server via `stdio`:

```bash
python -m src.databricks_mcp
```

An MCP client/host can then connect to this process via its standard input/output.

(Instructions for HTTP/SSE transport will be added if implemented).

## Development

1.  **Setup:** Follow the steps in the [Setup](#setup) section. Ensure development dependencies are installed (they are by default with `poetry install`).
2.  **Running Tests:**
    ```bash
    pytest
    ```
3.  **Linting/Formatting:**
    ```bash
    ruff check .
    ruff format .
    ```
4.  **Project Structure:** See the [Technical Architecture](docs/databricks_mcp_tech_arch.md#32-recommended-project-structure) document.
5.  **Adding Tools/Resources:**
    *   Create or modify Python files under `src/databricks_mcp/tools/` or `src/databricks_mcp/resources/`.
    *   Implement the logic using the `mcp` framework (`@mcp.tool()`, `@mcp.resource()`) and the `db_client.py` wrapper.
    *   Register the new capabilities in `src/databricks_mcp/server.py`.
    *   Add corresponding unit tests in the `tests/unit/` directory.

See the [Implementation Plan](docs/implementation-plan.md) for tracking development tasks.
The implementation based on the initial plan is now complete. 