import logging

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings."""

    # Load environment variables from .env file if it exists
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # Databricks Configuration
    # DATABRICKS_HOST is automatically picked up by databricks-sdk if set in env
    # DATABRICKS_TOKEN or other auth methods are also picked up by databricks-sdk
    # We mainly need settings specific to the MCP server itself.
    databricks_host: str | None = None # Explicitly define if needed, otherwise SDK handles it

    # Server Configuration
    log_level: str = "INFO"
    enable_get_secret: bool = False # Security-sensitive: default to False

    @property
    def numeric_log_level(self) -> int:
        """Convert log level string to numeric value."""
        return logging.getLevelName(self.log_level.upper())

# Single instance of settings to be imported by other modules
settings = Settings()
