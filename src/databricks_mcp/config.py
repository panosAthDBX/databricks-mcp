import os
from pydantic import BaseModel, Field, HttpUrl, ValidationError, field_validator
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

class DatabricksConfig(BaseModel):
    """Configuration settings for Databricks connection."""
    host: HttpUrl = Field(..., description="Databricks workspace host URL (e.g., https://adb-xxxxxxxxxxxxxxxx.xx.azuredatabricks.net)")
    token: str = Field(..., description="Databricks Personal Access Token (PAT) or other API token")
    # Add other auth methods later if needed (e.g., client_id, client_secret for OAuth/SP)

    @field_validator('host', mode='before')
    @classmethod
    def ensure_scheme(cls, v):
        if isinstance(v, str) and not v.startswith(('http://', 'https://')):
            logger.warning(f"Databricks host '{v}' missing scheme, assuming https.")
            return f"https://{v}"
        return v

def load_config() -> DatabricksConfig:
    """Loads configuration from environment variables.

    Loads variables from a .env file if present (primarily for development).
    Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.

    Returns:
        DatabricksConfig: An instance containing the loaded configuration.

    Raises:
        ValueError: If required environment variables are missing or invalid.
    """
    # Load .env file if it exists (useful for local development)
    load_dotenv()
    logger.info("Attempting to load configuration from environment variables...")

    try:
        config = DatabricksConfig(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )
        logger.info("Databricks configuration loaded successfully.")
        # Be careful not to log the token itself in production logging levels
        logger.debug(f"Loaded host: {config.host}")
        return config
    except ValidationError as e:
        logger.error(f"Configuration validation failed: {e}")
        missing_vars = []
        if not os.getenv("DATABRICKS_HOST"):
            missing_vars.append("DATABRICKS_HOST")
        if not os.getenv("DATABRICKS_TOKEN"):
            missing_vars.append("DATABRICKS_TOKEN")

        error_msg = "Failed to load configuration. "
        if missing_vars:
            error_msg += f"Missing required environment variables: {', '.join(missing_vars)}. "
        error_msg += "Please set them or create a .env file."

        raise ValueError(error_msg) from e

# Example usage (optional, for testing the module directly)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        # Create a dummy .env for testing if it doesn't exist
        if not os.path.exists('.env'):
            with open('.env', 'w') as f:
                f.write("DATABRICKS_HOST=https://your-workspace.databricks.com\n")
                f.write("DATABRICKS_TOKEN=dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n")
            logger.info("Created dummy .env file for testing.")

        loaded_settings = load_config()
        print("Configuration loaded:")
        print(f" Host: {loaded_settings.host}")
        # Avoid printing token directly in real scenarios
        print(f" Token: {'*' * (len(loaded_settings.token) - 4) + loaded_settings.token[-4:]}")

        # Clean up dummy file
        if os.path.exists('.env') and 'your-workspace' in open('.env').read():
             os.remove('.env')
             logger.info("Removed dummy .env file.")

    except ValueError as e:
        print(f"Error loading config: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}") 