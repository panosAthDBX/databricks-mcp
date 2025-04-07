# Product Requirements Document: Databricks MCP Server

## 1. Introduction

This document outlines the requirements for a Databricks Model Context Protocol (MCP) Server. The server will act as a standardized interface, enabling AI agents and applications (MCP Hosts/Clients) to interact securely and efficiently with Databricks workspaces, leveraging its capabilities for data management, AI/ML development, and workspace operations.

## 2. Goals

*   **Enable Agent Interaction:** Allow AI agents to programmatically interact with Databricks resources and APIs via the MCP standard.
*   **Standardized Access:** Provide a consistent and discoverable interface to key Databricks functionalities.
*   **Secure Operations:** Ensure all interactions adhere to Databricks security and governance models (authentication, permissions).
*   **Extensibility:** Design the server to be easily extendable to support new Databricks features and APIs as they become available.
*   **Developer Experience:** Provide a clear way for developers to understand and utilize the MCP server's capabilities.

## 3. Use Cases

*   **Agent-driven Development:** An agent uses the MCP server to create/modify Databricks notebooks, run code cells, manage libraries, and interact with Git repositories within Databricks.
*   **Data Retrieval & Analysis:** An agent queries database schemas, retrieves table previews, executes SQL queries against SQL Warehouses, and fetches data summaries via the MCP server.
*   **AI/ML Workflow Management:** An agent uses the server to list MLflow experiments, retrieve run details, register models, trigger model training jobs, or query deployed model endpoints.
*   **Workspace Management:** An agent lists clusters, checks cluster status, lists jobs, retrieves job run details, or manages workspace files (DBFS/Volumes) and secrets.
*   **RAG Implementation:** An agent uses the server to interact with Databricks Vector Search to index or query documents for Retrieval-Augmented Generation tasks.
*   **Infrastructure Management:** An agent manages SQL warehouses (start/stop/list).

## 4. Functional Requirements

The MCP server must expose Databricks functionalities through the standard MCP capabilities: Tools, Resources, and Prompts. Authentication with the Databricks workspace is a prerequisite for all operations.

### 4.1. Authentication

*   **REQ-AUTH-01:** The server MUST authenticate with the target Databricks workspace using standard Databricks authentication methods (e.g., Personal Access Tokens, OAuth, Service Principals). Configuration details MUST be securely managed (e.g., via environment variables or a secrets manager, not hardcoded).
*   **REQ-AUTH-02:** All subsequent API calls made by the server to Databricks MUST use the established authenticated context.
*   **REQ-AUTH-03:** The server MUST respect Databricks permissions; operations should fail gracefully with appropriate MCP error codes if the authenticated principal lacks the necessary permissions.

### 4.2. MCP Capabilities

The server should implement the following capabilities, categorized by Databricks area:

#### 4.2.1. Workspace & Development

*   **REQ-WS-TOOL-01 (Tool):** `run_notebook`: Execute a Databricks notebook.
    *   *Parameters:* `notebook_path` (string), `parameters` (object, optional), `cluster_id` (string, optional - defaults to a suitable running cluster if possible).
    *   *Returns:* `run_id` (string), `status` (string - e.g., PENDING, RUNNING, SUCCESS, FAILED). (Consider async handling - see Tech Arch).
*   **REQ-WS-TOOL-02 (Tool):** `execute_code`: Execute a snippet of code within a specified cluster context.
    *   *Parameters:* `code` (string), `language` (string - e.g., 'python', 'sql', 'scala', 'r'), `cluster_id` (string).
    *   *Returns:* `result` (string/object), `status` (string).
*   **REQ-WS-RES-01 (Resource):** `list_workspace_items`: List items (notebooks, folders, files, repos, libraries) within a specified workspace path.
    *   *Parameters:* `path` (string).
    *   *Returns:* List of items with `path`, `type`, `object_id`.
*   **REQ-WS-RES-02 (Resource):** `get_notebook_content`: Retrieve the content of a specified notebook.
    *   *Parameters:* `path` (string).
    *   *Returns:* `content` (string), `language` (string).
*   **REQ-WS-RES-03 (Resource):** `list_repos`: List configured Databricks Repos.
    *   *Parameters:* None.
    *   *Returns:* List of repos with `id`, `path`, `url`, `branch`.
*   **REQ-WS-RES-04 (Resource):** `get_repo_status`: Get the Git status of a specified Databricks Repo.
    *   *Parameters:* `repo_id` (string).
    *   *Returns:* `status` details (e.g., current branch, uncommitted changes).

#### 4.2.2. Compute

*   **REQ-COMP-RES-01 (Resource):** `list_clusters`: List available clusters.
    *   *Parameters:* None.
    *   *Returns:* List of clusters with `cluster_id`, `name`, `state`, `driver_node_type`, `worker_node_type`.
*   **REQ-COMP-RES-02 (Resource):** `get_cluster_details`: Get detailed information about a specific cluster.
    *   *Parameters:* `cluster_id` (string).
    *   *Returns:* Detailed cluster configuration and status.
*   **REQ-COMP-TOOL-01 (Tool):** `start_cluster`: Start a terminated cluster.
    *   *Parameters:* `cluster_id` (string).
    *   *Returns:* `status` (string - e.g., PENDING, RUNNING, ERROR).
*   **REQ-COMP-TOOL-02 (Tool):** `terminate_cluster`: Terminate a running cluster.
    *   *Parameters:* `cluster_id` (string).
    *   *Returns:* `status` (string - e.g., TERMINATING, TERMINATED, ERROR).

#### 4.2.3. Data & SQL

*   **REQ-DATA-TOOL-01 (Tool):** `execute_sql`: Execute a SQL query against a specified SQL Warehouse.
    *   *Parameters:* `sql_query` (string), `warehouse_id` (string), `catalog` (string, optional), `schema` (string, optional).
    *   *Returns:* `statement_id` (string), `status` (string). (Consider async handling - see Tech Arch & `get_statement_result` tool).
*   **REQ-DATA-TOOL-02 (Tool):** `get_statement_result`: Retrieve results for a previously executed SQL statement.
    *   *Parameters:* `statement_id` (string).
    *   *Returns:* `result_data` (object/array), `status` (string), `schema` (object, optional).
*   **REQ-DATA-RES-01 (Resource):** `list_catalogs`: List available Unity Catalogs.
    *   *Parameters:* None.
    *   *Returns:* List of catalogs with `name`, `comment`.
*   **REQ-DATA-RES-02 (Resource):** `list_schemas`: List schemas within a specified catalog.
    *   *Parameters:* `catalog_name` (string).
    *   *Returns:* List of schemas with `name`, `comment`.
*   **REQ-DATA-RES-03 (Resource):** `list_tables`: List tables/views within a specified schema.
    *   *Parameters:* `catalog_name` (string), `schema_name` (string).
    *   *Returns:* List of tables with `name`, `type` (TABLE, VIEW), `comment`.
*   **REQ-DATA-RES-04 (Resource):** `get_table_schema`: Retrieve the schema definition for a specified table.
    *   *Parameters:* `catalog_name` (string), `schema_name` (string), `table_name` (string).
    *   *Returns:* Table schema information (column names, types).
*   **REQ-DATA-RES-05 (Resource):** `preview_table`: Retrieve the first N rows of a specified table.
    *   *Parameters:* `catalog_name` (string), `schema_name` (string), `table_name` (string), `row_limit` (int, default 100).
    *   *Returns:* Array of rows.
*   **REQ-DATA-RES-06 (Resource):** `list_sql_warehouses`: List available SQL Warehouses.
    *   *Parameters:* None.
    *   *Returns:* List of warehouses with `id`, `name`, `state`, `cluster_size`.
*   **REQ-DATA-TOOL-03 (Tool):** `start_sql_warehouse`: Start a stopped SQL Warehouse.
    *   *Parameters:* `warehouse_id` (string).
    *   *Returns:* `status` (string).
*   **REQ-DATA-TOOL-04 (Tool):** `stop_sql_warehouse`: Stop a running SQL Warehouse.
    *   *Parameters:* `warehouse_id` (string).
    *   *Returns:* `status` (string).

#### 4.2.4. AI & Machine Learning (Mosaic AI / MLflow)

*   **REQ-ML-RES-01 (Resource):** `list_mlflow_experiments`: List MLflow experiments.
    *   *Parameters:* None.
    *   *Returns:* List of experiments with `experiment_id`, `name`.
*   **REQ-ML-RES-02 (Resource):** `list_mlflow_runs`: List runs for a given MLflow experiment.
    *   *Parameters:* `experiment_id` (string), `filter_string` (string, optional), `max_results` (int, optional).
    *   *Returns:* List of runs with `run_id`, `status`, `start_time`.
*   **REQ-ML-RES-03 (Resource):** `get_mlflow_run_details`: Get parameters, metrics, and artifacts for a specific MLflow run.
    *   *Parameters:* `run_id` (string).
    *   *Returns:* Object containing run details.
*   **REQ-ML-RES-04 (Resource):** `list_registered_models`: List models registered in the MLflow Model Registry.
    *   *Parameters:* `filter_string` (string, optional), `max_results` (int, optional).
    *   *Returns:* List of models with `name`, `latest_versions`.
*   **REQ-ML-RES-05 (Resource):** `get_model_version_details`: Get details for a specific model version.
    *   *Parameters:* `model_name` (string), `version` (string).
    *   *Returns:* Object containing model version details.
*   **REQ-ML-TOOL-01 (Tool):** `query_model_serving_endpoint`: Send data to a Databricks Model Serving endpoint and get predictions.
    *   *Parameters:* `endpoint_name` (string), `input_data` (object).
    *   *Returns:* `predictions` (object).
*   **REQ-ML-TOOL-02 (Tool - RAG):** `add_to_vector_index`: Add/update documents in a Databricks Vector Search index.
    *   *Parameters:* `index_name` (string), `documents` (array of objects).
    *   *Returns:* `status` (string), `num_added` (int).
*   **REQ-ML-TOOL-03 (Tool - RAG):** `query_vector_index`: Query a Databricks Vector Search index.
    *   *Parameters:* `index_name` (string), `query_vector` (array of floats) or `query_text` (string), `num_results` (int, default 10).
    *   *Returns:* `results` (array of objects with similarity scores).

#### 4.2.5. Jobs

*   **REQ-JOB-RES-01 (Resource):** `list_jobs`: List configured Databricks Jobs.
    *   *Parameters:* `name_filter` (string, optional).
    *   *Returns:* List of jobs with `job_id`, `name`, `schedule`.
*   **REQ-JOB-RES-02 (Resource):** `get_job_details`: Get the configuration of a specific job.
    *   *Parameters:* `job_id` (string).
    *   *Returns:* Job settings and task details.
*   **REQ-JOB-RES-03 (Resource):** `list_job_runs`: List recent runs for a specific job.
    *   *Parameters:* `job_id` (string), `limit` (int, optional), `status_filter` (string, optional - e.g., 'SUCCESS', 'FAILED').
    *   *Returns:* List of job runs with `run_id`, `start_time`, `end_time`, `state`.
*   **REQ-JOB-TOOL-01 (Tool):** `run_job_now`: Trigger a specific job to run immediately.
    *   *Parameters:* `job_id` (string), `notebook_params` (object, optional), `python_params` (array of strings, optional), etc.
    *   *Returns:* `run_id` (string), `status` (string).

#### 4.2.6. File Management (DBFS & Volumes)

*   **REQ-FILE-RES-01 (Resource):** `list_files`: List files and directories in DBFS or a Unity Catalog Volume path.
    *   *Parameters:* `path` (string - e.g., '/mnt/...', '/Volumes/.../...')
    *   *Returns:* List of items with `path`, `is_dir`, `size`.
*   **REQ-FILE-TOOL-01 (Tool):** `read_file`: Read the content of a file from DBFS or a Volume.
    *   *Parameters:* `path` (string), `offset` (int, optional), `length` (int, optional).
    *   *Returns:* `content` (string, potentially base64 encoded for binary), `bytes_read` (int).
*   **REQ-FILE-TOOL-02 (Tool):** `write_file`: Write content to a file in DBFS or a Volume.
    *   *Parameters:* `path` (string), `content` (string, potentially base64), `overwrite` (boolean, default false).
    *   *Returns:* `status` (string), `bytes_written` (int).
*   **REQ-FILE-TOOL-03 (Tool):** `delete_file`: Delete a file or directory (recursively) from DBFS or a Volume.
    *   *Parameters:* `path` (string), `recursive` (boolean, default false for directories).
    *   *Returns:* `status` (string).
*   **REQ-FILE-TOOL-04 (Tool):** `create_directory`: Create a directory in DBFS or a Volume.
    *   *Parameters:* `path` (string).
    *   *Returns:* `status` (string).

#### 4.2.7. Secrets Management

*   **REQ-SEC-RES-01 (Resource):** `list_secret_scopes`: List available secret scopes.
    *   *Parameters:* None.
    *   *Returns:* List of scope names.
*   **REQ-SEC-RES-02 (Resource):** `list_secrets`: List secret keys within a scope (does *not* return values).
    *   *Parameters:* `scope_name` (string).
    *   *Returns:* List of secret keys.
*   **REQ-SEC-TOOL-01 (Tool):** `get_secret`: Retrieve the value of a secret (requires appropriate permissions). **Use with extreme caution.** Consider if this capability should be enabled by default.
    *   *Parameters:* `scope_name` (string), `key` (string).
    *   *Returns:* `secret_value` (string, potentially redacted or requiring further confirmation).
*   **REQ-SEC-TOOL-02 (Tool):** `put_secret`: Create or update a secret.
    *   *Parameters:* `scope_name` (string), `key` (string), `secret_value` (string).
    *   *Returns:* `status` (string).
*   **REQ-SEC-TOOL-03 (Tool):** `delete_secret`: Delete a secret.
    *   *Parameters:* `scope_name` (string), `key` (string).
    *   *Returns:* `status` (string).

### 4.3. Prompts

*   **REQ-PROMPT-01:** The server MAY define specific MCP Prompts to guide AI agents on how to effectively use complex tools or common workflows (e.g., a prompt template for analyzing query performance, reviewing notebook changes, or creating a standard MLflow experiment setup).

## 5. Non-Functional Requirements

*   **NFR-PERF-01:** The server should respond to MCP client requests in a timely manner. Latency should primarily be dictated by the underlying Databricks API call duration. Blocking operations should be documented.
*   **NFR-SEC-01:** Authentication credentials MUST be stored and handled securely (e.g., env vars, secrets manager). No sensitive information (credentials, secret values unless explicitly requested via `get_secret`) should be logged. Input validation should be performed.
*   **NFR-SEC-02 (Rate Limiting):** The server SHOULD implement configurable rate limiting per client/principal to prevent abuse and hitting Databricks API limits.
*   **NFR-REL-01:** The server must handle potential Databricks API errors gracefully (e.g., network issues, throttling, invalid requests) and report standardized, meaningful MCP error codes and messages back to the MCP client.
*   **NFR-REL-02 (Logging):** The server MUST implement structured logging (e.g., JSON format) with configurable levels (DEBUG, INFO, WARN, ERROR). Logs should include correlation IDs for tracing requests. Sensitive data must be masked.
*   **NFR-MAINT-01:** The codebase must be well-structured (see Tech Arch), documented (docstrings, README), and include comprehensive unit and integration tests (>80% coverage target). Follow defined coding standards (e.g., Ruff).
*   **NFR-COMP-01:** The server must implement the MCP protocol specification correctly, supporting discovery and invocation via specified transports (initially `stdio`, potentially `HTTP/SSE`).
*   **NFR-CONFIG-01:** All necessary configurations (Databricks host, auth method, credentials path/env vars, log level, rate limits) MUST be externalized from the code (e.g., environment variables, config file).

## 6. Future Considerations

*   Support for additional Databricks APIs (e.g., Delta Sharing, detailed Governance controls like Grants, Metastore management).
*   More sophisticated prompt engineering examples and potentially dynamic prompt generation based on context.
*   Support for real-time notifications/streaming from Databricks (e.g., job status updates) if feasible via APIs and MCP enhancements.
*   Integration with Databricks CLI configuration profiles for easier local development setup.
*   Support for creating/updating resources (e.g., create job, update cluster config).
*   Asynchronous handling for all long-running operations.

## 7. Out of Scope

*   Providing a graphical user interface.
*   Managing Databricks user permissions (relies on existing Databricks setup).
*   Complex caching strategies beyond basic memoization for highly static resources.
*   Direct execution of arbitrary code not submitted via specific tools like `execute_code` or `run_notebook`.
*   Direct manipulation of cluster libraries outside of job/notebook definitions. 