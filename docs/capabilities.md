# Databricks MCP Server Capabilities

This document outlines the tools and resources provided by the Databricks Multi-Capability Platform (MCP) Server. It is intended for product managers, architects, and developers who need to understand the server's functionality for integration and product definition.

The server exposes capabilities through two main mechanisms:

1.  **Tools:** Actions that perform operations within Databricks (e.g., running a notebook, executing SQL, starting a cluster). Tools often represent verbs.
2.  **Resources:** Read-only accessors for retrieving information about Databricks assets (e.g., listing clusters, getting table schemas, listing jobs). Resources often represent nouns and follow RESTful patterns.

Capabilities are grouped by Databricks area (Compute, Data, Files, Jobs, ML, Secrets, Workspace).

---

## Compute Capabilities

Manage Databricks compute resources (Clusters).

**Tools:**

*   `databricks:compute:start_cluster`
    *   **Description:** Starts a terminated Databricks cluster. Waits for completion.
    *   **Args:** `cluster_id` (str)
    *   **Returns:** Status dictionary.
*   `databricks:compute:terminate_cluster`
    *   **Description:** Terminates a running Databricks cluster. Waits for completion.
    *   **Args:** `cluster_id` (str)
    *   **Returns:** Status dictionary.

**Resources:**

*   `databricks:compute:list_clusters`
    *   **Description:** Lists all available Databricks clusters in the workspace.
    *   **Returns:** List of cluster summaries (ID, name, state, node types).
*   `databricks:compute:get_cluster_details/{cluster_id}`
    *   **Description:** Gets detailed information about a specific Databricks cluster.
    *   **Args:** `cluster_id` (str)
    *   **Returns:** Detailed dictionary of cluster configuration and state.

---

## Data Capabilities (Unity Catalog & SQL Warehouses)

Interact with Unity Catalog metadata and execute SQL queries.

**Tools:**

*   `databricks:sql:execute_statement`
    *   **Description:** Submits a SQL query for asynchronous execution against a specified SQL Warehouse. Returns a `statement_id`.
    *   **Args:** `sql_query` (str), `warehouse_id` (str), `catalog` (str, optional), `schema` (str, optional)
    *   **Returns:** Dictionary with `statement_id` and initial status.
*   `databricks:sql:get_statement_result`
    *   **Description:** Retrieves the status and results for a previously submitted SQL statement (`statement_id`). Handles different result dispositions (inline, external links).
    *   **Args:** `statement_id` (str)
    *   **Returns:** Dictionary with status, schema (if available), result data (list of dicts or arrays), and error message (if failed).
*   `databricks:sql:start_warehouse`
    *   **Description:** Starts a stopped Databricks SQL Warehouse. Waits for completion.
    *   **Args:** `warehouse_id` (str)
    *   **Returns:** Status dictionary.
*   `databricks:sql:stop_warehouse`
    *   **Description:** Stops a running Databricks SQL Warehouse. Waits for completion.
    *   **Args:** `warehouse_id` (str)
    *   **Returns:** Status dictionary.

**Resources:**

*   `databricks:uc:list_catalogs`
    *   **Description:** Lists available Unity Catalogs.
    *   **Returns:** List of catalog summaries (name, comment, owner).
*   `databricks:uc:list_schemas/{catalog_name}`
    *   **Description:** Lists schemas (databases) within a specified Unity Catalog.
    *   **Args:** `catalog_name` (str)
    *   **Returns:** List of schema summaries (name, catalog, comment, owner).
*   `databricks:uc:list_tables/{catalog_name}/{schema_name}`
    *   **Description:** Lists tables and views within a specified schema.
    *   **Args:** `catalog_name` (str), `schema_name` (str)
    *   **Returns:** List of table/view summaries (name, catalog, schema, type, comment, owner).
*   `databricks:uc:get_table_schema/{catalog_name}/{schema_name}/{table_name}`
    *   **Description:** Retrieves the schema (column names and types) for a specific table or view.
    *   **Args:** `catalog_name` (str), `schema_name` (str), `table_name` (str)
    *   **Returns:** Dictionary with table details and list of column definitions.
*   `databricks:uc:preview_table/{catalog_name}/{schema_name}/{table_name}/{row_limit}`
    *   **Description:** Retrieves the first N rows (default 100) of a table/view using a running SQL Warehouse.
    *   **Args:** `catalog_name` (str), `schema_name` (str), `table_name` (str), `row_limit` (int, optional)
    *   **Returns:** List of dictionaries representing rows. Requires a running SQL Warehouse.
*   `databricks:sql:list_warehouses`
    *   **Description:** Lists available Databricks SQL Warehouses.
    *   **Returns:** List of warehouse summaries (ID, name, state, size, etc.).

---

## File Capabilities (DBFS & Volumes)

Interact with the Databricks File System (DBFS) and Unity Catalog Volumes. Note: These primarily use the DBFS API; Volume interaction might have limitations.

**Tools:**

*   `databricks:files:read`
    *   **Description:** Reads content from a file path. Content is returned base64 encoded.
    *   **Args:** `path` (str), `offset` (int, optional), `length` (int, optional, default 1MB)
    *   **Returns:** Dictionary with path, base64 content, and bytes read.
*   `databricks:files:write`
    *   **Description:** Writes base64 encoded content to a file path.
    *   **Args:** `path` (str), `content_base64` (str), `overwrite` (bool, optional, default False)
    *   **Returns:** Status dictionary with bytes written.
*   `databricks:files:delete`
    *   **Description:** Deletes a file or directory.
    *   **Args:** `path` (str), `recursive` (bool, optional, default False)
    *   **Returns:** Status dictionary.
*   `databricks:files:create_directory`
    *   **Description:** Creates a directory, including parent directories if needed.
    *   **Args:** `path` (str)
    *   **Returns:** Status dictionary.

**Resources:**

*   `databricks:files:list/{path}`
    *   **Description:** Lists files and directories in a specified DBFS or Volume path.
    *   **Args:** `path` (str)
    *   **Returns:** List of file/directory summaries (path, is_dir, size).

---

## Job Capabilities

Manage and interact with Databricks Jobs.

**Tools:**

*   `databricks:jobs:run_now`
    *   **Description:** Triggers a job to run immediately and waits for completion. Allows overriding parameters.
    *   **Args:** `job_id` (int), `notebook_params` (dict, optional), `python_params` (list[str], optional), `jar_params` (list[str], optional), `spark_submit_params` (list[str], optional)
    *   **Returns:** Dictionary with run details (run_id, status, result_state, URL).

**Resources:**

*   `databricks:jobs:list_jobs/{name_filter}/{limit}`
    *   **Description:** Lists configured Databricks Jobs.
    *   **Args:** `name_filter` (str, optional), `limit` (int, optional, default 20)
    *   **Returns:** List of job summaries (ID, name, creator, schedule).
*   `databricks:jobs:get_job_details/{job_id}`
    *   **Description:** Gets the detailed configuration of a specific job.
    *   **Args:** `job_id` (int)
    *   **Returns:** Dictionary containing full job settings.
*   `databricks:jobs:list_job_runs/{job_id}/{limit}/{status_filter}`
    *   **Description:** Lists recent runs for a specific job.
    *   **Args:** `job_id` (int), `limit` (int, optional, default 25), `status_filter` (str, optional e.g., 'TERMINATED')
    *   **Returns:** List of run summaries (ID, job_id, times, state, URL).

---

## Machine Learning Capabilities (MLflow, Serving, Vector Search)

Interact with MLflow, Model Serving Endpoints, and Vector Search Indexes.

**Tools:**

*   `databricks:ml:query_model_serving_endpoint`
    *   **Description:** Queries a Databricks Model Serving endpoint.
    *   **Args:** `endpoint_name` (str), `input_data` (dict | list)
    *   **Returns:** Dictionary containing the model's predictions.
*   `databricks:vs:add_to_index`
    *   **Description:** Adds or updates documents in a Databricks Vector Search index.
    *   **Args:** `index_name` (str), `primary_key` (str), `documents` (list[dict])
    *   **Returns:** Status dictionary with number added/updated. Requires `databricks-vectorsearch` library.
*   `databricks:vs:query_index`
    *   **Description:** Queries a Databricks Vector Search index for similar documents. Requires either `query_vector` or `query_text`.
    *   **Args:** `index_name` (str), `columns` (list[str]), `query_vector` (list[float], optional), `query_text` (str, optional), `num_results` (int, optional, default 10), `filters_json` (str, optional), `query_type` (str, optional, default 'ANN')
    *   **Returns:** Dictionary with results and manifest. Requires `databricks-vectorsearch` library.

**Resources:**

*   `databricks:mlflow:list_experiments/{max_results}`
    *   **Description:** Lists MLflow experiments.
    *   **Args:** `max_results` (int, optional, default 100)
    *   **Returns:** List of experiment summaries.
*   `databricks:mlflow:list_runs/{experiment_id}/{filter_string}/{max_results}`
    *   **Description:** Lists runs for a given MLflow experiment.
    *   **Args:** `experiment_id` (str), `filter_string` (str, optional), `max_results` (int, optional, default 100)
    *   **Returns:** List of run summaries.
*   `databricks:mlflow:get_run_details/{run_id}`
    *   **Description:** Gets parameters, metrics, and tags for a specific MLflow run.
    *   **Args:** `run_id` (str)
    *   **Returns:** Dictionary with detailed run information.
*   `databricks:mlflow:list_registered_models/{filter_string}/{max_results}`
    *   **Description:** Lists models in the MLflow Model Registry.
    *   **Args:** `filter_string` (str, optional), `max_results` (int, optional, default 100)
    *   **Returns:** List of registered model summaries.
*   `databricks:mlflow:get_model_version_details/{model_name}/{version}`
    *   **Description:** Gets details for a specific version of a registered model.
    *   **Args:** `model_name` (str), `version` (str)
    *   **Returns:** Dictionary with detailed model version information.

---

## Secrets Capabilities

Manage Databricks secrets. Use with caution.

**Tools:**

*   `databricks:secrets:get_secret`
    *   **Description:** Retrieves the value of a secret. **WARNING:** Exposes sensitive information. Enabled/disabled via server configuration (`enable_get_secret`).
    *   **Args:** `scope_name` (str), `key` (str)
    *   **Returns:** Dictionary with scope, key, and secret value (string or base64 encoded). Raises `PermissionError` if disabled.
*   `databricks:secrets:put_secret`
    *   **Description:** Creates or updates a secret with a string value.
    *   **Args:** `scope_name` (str), `key` (str), `secret_value` (str)
    *   **Returns:** Status dictionary.
*   `databricks:secrets:delete_secret`
    *   **Description:** Deletes a secret.
    *   **Args:** `scope_name` (str), `key` (str)
    *   **Returns:** Status dictionary.

**Resources:**

*   `databricks:secrets:list_scopes`
    *   **Description:** Lists available secret scopes.
    *   **Returns:** List of scope summaries (name).
*   `databricks:secrets:list_secrets/{scope_name}`
    *   **Description:** Lists secret keys within a scope (values are NOT returned).
    *   **Args:** `scope_name` (str)
    *   **Returns:** List of secret key summaries (key, timestamp).

---

## Workspace Capabilities

Interact with Workspace assets like notebooks and repositories.

**Tools:**

*   `databricks:workspace:run_notebook`
    *   **Description:** Runs a Databricks notebook and waits for completion. Uses the Jobs API `run_now`.
    *   **Args:** `notebook_path` (str), `cluster_id` (str, optional), `parameters` (dict, optional)
    *   **Returns:** Dictionary with run details (run_id, status, result_state, URL).
*   `databricks:workspace:execute_code`
    *   **Description:** Executes a snippet of code (Python, SQL, Scala, R) on a cluster and waits for completion.
    *   **Args:** `code` (str), `language` (Literal["python", "sql", "scala", "r"]), `cluster_id` (str)
    *   **Returns:** Dictionary with command results (ID, status, result type, result data/error cause).

**Resources:**

*   `databricks:workspace:list_items/{path}`
    *   **Description:** Lists items (notebooks, folders, files, repos) within a workspace path.
    *   **Args:** `path` (str)
    *   **Returns:** List of item summaries (path, type, object_id).
*   `databricks:workspace:get_notebook_content/{path}`
    *   **Description:** Retrieves the decoded content of a notebook.
    *   **Args:** `path` (str)
    *   **Returns:** Dictionary with path, decoded content, and language.
*   `databricks:repos:list`
    *   **Description:** Lists configured Databricks Repos.
    *   **Returns:** List of repo summaries (ID, path, URL, branch, commit).
*   `databricks:repos:get_status/{repo_id}`
    *   **Description:** Gets the status (branch, commit) of a specific Databricks Repo.
    *   **Args:** `repo_id` (str)
    *   **Returns:** Dictionary with repo status details.

--- 