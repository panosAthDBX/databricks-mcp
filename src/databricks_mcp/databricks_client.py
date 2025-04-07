import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from functools import wraps

logger = logging.getLogger(__name__)

# Decorator for handling common Databricks API errors
def handle_databricks_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabricksError as e:
            # Log the specific Databricks error
            logger.error(f"Databricks API error in {func.__name__}: {e}", exc_info=True)
            # Re-raise as a more generic exception or a custom one if needed later
            # For now, let's re-raise to be handled by MCP server framework
            raise ConnectionError(f"Databricks API error: {e}") from e
        except Exception as e:
            # Catch other unexpected errors during API interaction
            logger.error(f"Unexpected error in {func.__name__} calling Databricks API: {e}", exc_info=True)
            raise RuntimeError(f"Unexpected error interacting with Databricks: {e}") from e
    return wrapper

class DatabricksAPIClient:
    """A wrapper client for interacting with the Databricks SDK."""

    def __init__(self, workspace_client: WorkspaceClient):
        """Initializes the client with an authenticated WorkspaceClient."""
        if not isinstance(workspace_client, WorkspaceClient):
            raise TypeError("workspace_client must be an instance of databricks.sdk.WorkspaceClient")
        self.client = workspace_client
        logger.info("DatabricksAPIClient initialized.")

    # --- Workspace Methods --- #
    @handle_databricks_errors
    def list_workspace_items(self, path: str):
        logger.debug(f"Listing workspace items for path: {path}")
        # Implementation using self.client.workspace.list(...) will go here
        raise NotImplementedError("list_workspace_items not yet implemented")

    @handle_databricks_errors
    def get_notebook_content(self, path: str):
        logger.debug(f"Getting notebook content for path: {path}")
        raise NotImplementedError("get_notebook_content not yet implemented")

    @handle_databricks_errors
    def list_repos(self):
        logger.debug("Listing repos")
        raise NotImplementedError("list_repos not yet implemented")

    @handle_databricks_errors
    def get_repo_status(self, repo_id: str):
        logger.debug(f"Getting repo status for repo_id: {repo_id}")
        raise NotImplementedError("get_repo_status not yet implemented")

    @handle_databricks_errors
    def run_notebook(self, path: str, parameters: dict | None = None):
        logger.debug(f"Running notebook: {path} with params: {parameters}")
        raise NotImplementedError("run_notebook not yet implemented")

    @handle_databricks_errors
    def execute_code(self, cluster_id: str, language: str, code: str):
        logger.debug(f"Executing code on cluster {cluster_id}")
        raise NotImplementedError("execute_code not yet implemented")

    # --- Compute Methods --- #
    @handle_databricks_errors
    def list_clusters(self):
        logger.debug("Listing clusters")
        # Example: return self.client.clusters.list()
        raise NotImplementedError("list_clusters not yet implemented")

    @handle_databricks_errors
    def get_cluster_details(self, cluster_id: str):
        logger.debug(f"Getting details for cluster_id: {cluster_id}")
        raise NotImplementedError("get_cluster_details not yet implemented")

    @handle_databricks_errors
    def start_cluster(self, cluster_id: str):
        logger.debug(f"Starting cluster_id: {cluster_id}")
        raise NotImplementedError("start_cluster not yet implemented")

    @handle_databricks_errors
    def terminate_cluster(self, cluster_id: str):
        logger.debug(f"Terminating cluster_id: {cluster_id}")
        raise NotImplementedError("terminate_cluster not yet implemented")

    # --- Data & SQL Methods --- #
    @handle_databricks_errors
    def list_catalogs(self):
        logger.debug("Listing catalogs")
        raise NotImplementedError("list_catalogs not yet implemented")

    @handle_databricks_errors
    def list_schemas(self, catalog_name: str):
        logger.debug(f"Listing schemas for catalog: {catalog_name}")
        raise NotImplementedError("list_schemas not yet implemented")

    @handle_databricks_errors
    def list_tables(self, catalog_name: str, schema_name: str):
        logger.debug(f"Listing tables for {catalog_name}.{schema_name}")
        raise NotImplementedError("list_tables not yet implemented")

    @handle_databricks_errors
    def get_table_schema(self, full_table_name: str):
        logger.debug(f"Getting schema for table: {full_table_name}")
        raise NotImplementedError("get_table_schema not yet implemented")

    @handle_databricks_errors
    def preview_table(self, full_table_name: str, max_rows: int = 100):
        logger.debug(f"Previewing table: {full_table_name}")
        raise NotImplementedError("preview_table not yet implemented")

    @handle_databricks_errors
    def execute_sql(self, statement: str, warehouse_id: str):
        logger.debug(f"Executing SQL on warehouse {warehouse_id}")
        raise NotImplementedError("execute_sql not yet implemented")

    # --- AI/ML Methods --- #
    @handle_databricks_errors
    def list_mlflow_experiments(self):
        logger.debug("Listing MLflow experiments")
        raise NotImplementedError("list_mlflow_experiments not yet implemented")

    @handle_databricks_errors
    def list_mlflow_runs(self, experiment_id: str):
        logger.debug(f"Listing runs for experiment_id: {experiment_id}")
        raise NotImplementedError("list_mlflow_runs not yet implemented")

    @handle_databricks_errors
    def get_mlflow_run_details(self, run_id: str):
        logger.debug(f"Getting details for run_id: {run_id}")
        raise NotImplementedError("get_mlflow_run_details not yet implemented")

    @handle_databricks_errors
    def list_registered_models(self):
        logger.debug("Listing registered models")
        raise NotImplementedError("list_registered_models not yet implemented")

    @handle_databricks_errors
    def get_model_version_details(self, model_name: str, version: str):
        logger.debug(f"Getting details for model: {model_name} version: {version}")
        raise NotImplementedError("get_model_version_details not yet implemented")

    @handle_databricks_errors
    def query_model_serving_endpoint(self, endpoint_name: str, data: dict):
        logger.debug(f"Querying model serving endpoint: {endpoint_name}")
        raise NotImplementedError("query_model_serving_endpoint not yet implemented")

    @handle_databricks_errors
    def add_to_vector_index(self, index_name: str, documents: list):
        logger.debug(f"Adding documents to vector index: {index_name}")
        raise NotImplementedError("add_to_vector_index not yet implemented")

    @handle_databricks_errors
    def query_vector_index(self, index_name: str, query_text: str, num_results: int = 5):
        logger.debug(f"Querying vector index: {index_name}")
        raise NotImplementedError("query_vector_index not yet implemented")

    # --- Job Methods --- #
    @handle_databricks_errors
    def list_jobs(self):
        logger.debug("Listing jobs")
        raise NotImplementedError("list_jobs not yet implemented")

    @handle_databricks_errors
    def get_job_details(self, job_id: int):
        logger.debug(f"Getting details for job_id: {job_id}")
        raise NotImplementedError("get_job_details not yet implemented")

    @handle_databricks_errors
    def list_job_runs(self, job_id: int):
        logger.debug(f"Listing runs for job_id: {job_id}")
        raise NotImplementedError("list_job_runs not yet implemented")

    @handle_databricks_errors
    def run_job_now(self, job_id: int, parameters: dict | None = None):
        logger.debug(f"Running job_id: {job_id} with params: {parameters}")
        raise NotImplementedError("run_job_now not yet implemented") 