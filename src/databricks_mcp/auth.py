import logging
from databricks.sdk import WorkspaceClient
from .config import DatabricksConfig

logger = logging.getLogger(__name__)

def get_authenticated_client(config: DatabricksConfig) -> WorkspaceClient:
    """Initializes and returns an authenticated Databricks WorkspaceClient.

    Currently supports Personal Access Token (PAT) authentication based on the
    provided configuration.

    Args:
        config: The loaded Databricks configuration.

    Returns:
        An authenticated WorkspaceClient instance.

    Raises:
        ConnectionError: If authentication fails.
        ValueError: If the configuration is unsuitable for authentication.
    """
    if not config.host or not config.token:
        error_msg = "Databricks host and token must be provided in the configuration for PAT authentication."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Attempting to authenticate to Databricks host: {config.host} using PAT...")

    try:
        # The SDK handles PAT authentication automatically when host and token are provided
        client = WorkspaceClient(host=str(config.host), token=config.token)

        # Perform a simple API call to verify authentication
        try:
            current_user = client.current_user.me()
            logger.info(f"Successfully authenticated to Databricks as user: {current_user.user_name}")
            return client
        except Exception as e:
            logger.error(f"Databricks authentication check failed: {e}", exc_info=True)
            raise ConnectionError(f"Failed to verify authentication with Databricks: {e}") from e

    except Exception as e:
        # Catch potential issues during WorkspaceClient initialization itself
        logger.error(f"Failed to initialize Databricks WorkspaceClient: {e}", exc_info=True)
        raise ConnectionError(f"Failed to initialize Databricks client: {e}") from e

# Example usage (optional, for testing the module directly)
if __name__ == '__main__':
    import sys
    from .config import load_config

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    try:
        test_config = load_config()
        print("\nAttempting to get authenticated client...")
        workspace_client = get_authenticated_client(test_config)
        print("Successfully obtained authenticated WorkspaceClient.")
        # You could add a simple API call here to test further, e.g.:
        # print(f"Listing root workspace dir: {workspace_client.workspace.list('/')}")

    except ValueError as e:
        print(f"Configuration Error: {e}")
    except ConnectionError as e:
        print(f"Authentication/Connection Error: {e}")
        print("Ensure your DATABRICKS_HOST and DATABRICKS_TOKEN in .env are correct and the token is valid.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}") 