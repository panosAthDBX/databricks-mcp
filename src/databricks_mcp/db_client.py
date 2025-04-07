import structlog
from databricks.sdk import WorkspaceClient

log = structlog.get_logger(__name__)

_db_client = None

def get_db_client() -> WorkspaceClient:
    """
    Initializes and returns a singleton Databricks WorkspaceClient.

    Relies on the databricks-sdk's default authentication mechanisms
    (environment variables, config files, etc.).
    See: https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    """
    global _db_client
    if _db_client is None:
        try:
            log.debug("Initializing Databricks WorkspaceClient...")
            # WorkspaceClient automatically picks up DATABRICKS_HOST, DATABRICKS_TOKEN
            # or other configured authentication methods.
            _db_client = WorkspaceClient()
            # Perform a simple check to ensure the client is functional
            _db_client.current_user.me()
            log.info("Databricks WorkspaceClient initialized successfully.")
        except Exception as e:
            log.error("Failed to initialize Databricks WorkspaceClient", error=str(e), exc_info=True)
            # Re-raise the exception to prevent the server from starting incorrectly
            raise RuntimeError("Could not initialize Databricks client. Check credentials and host.") from e
    return _db_client

# Example usage (optional, for testing):
# if __name__ == "__main__":
#     # Ensure environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN) are set
#     # or other auth is configured for this to work.
#     from config import settings # Adjust import if needed
#     from logging_config import setup_logging
#     setup_logging()
#     try:
#         client = get_db_client()
#         user = client.current_user.me()
#         print(f"Successfully connected as: {user.user_name}")
#     except Exception as e:
#         print(f"Error connecting: {e}")
