import os
import pytest
from pydantic import ValidationError
from unittest.mock import patch
from dotenv import load_dotenv

from databricks_mcp.config import load_config, DatabricksConfig

# Test valid configuration loading
@patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.databricks.com", "DATABRICKS_TOKEN": "test_token"})
def test_load_config_success():
    config = load_config()
    assert isinstance(config, DatabricksConfig)
    assert str(config.host) == "https://test.databricks.com/"
    assert config.token == "test_token"

# Test missing host
@patch.dict(os.environ, {"DATABRICKS_TOKEN": "test_token"}, clear=True)
def test_load_config_missing_host():
    with pytest.raises(ValueError, match="Missing required environment variables: DATABRICKS_HOST"):
        load_config()

# Test missing token
@patch.dict(os.environ, {"DATABRICKS_HOST": "https://test.databricks.com"}, clear=True)
def test_load_config_missing_token():
    with pytest.raises(ValueError, match="Missing required environment variables: DATABRICKS_TOKEN"):
        load_config()

# Test missing both
@patch.dict(os.environ, {}, clear=True)
def test_load_config_missing_both():
    with pytest.raises(ValueError, match="Missing required environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN"):
        load_config()

# Test invalid host URL (Pydantic validation)
@patch.dict(os.environ, {"DATABRICKS_HOST": "http://", "DATABRICKS_TOKEN": "test_token"})
def test_load_config_invalid_host():
    # Pydantic v2 is stricter, "http://" is invalid as it needs a host part.
    # Expecting ValueError because our load_config catches ValidationError and raises ValueError
    with pytest.raises(ValueError, match="Failed to load configuration."):
        load_config()

# Test adding https scheme if missing
@patch.dict(os.environ, {"DATABRICKS_HOST": "test-no-scheme.databricks.com", "DATABRICKS_TOKEN": "test_token"})
def test_load_config_adds_scheme():
    config = load_config()
    assert str(config.host) == "https://test-no-scheme.databricks.com/"

# Test loading from .env file (requires a temporary .env file)
@pytest.fixture
def temp_env_file(tmp_path):
    env_content = "DATABRICKS_HOST=https://env.databricks.com\nDATABRICKS_TOKEN=env_token"
    env_file = tmp_path / ".env"
    env_file.write_text(env_content)
    yield str(env_file) # Yield the path to the file

@patch.dict(os.environ, {}, clear=True) # Ensure env vars don't interfere
def test_load_config_from_dotenv(temp_env_file):
    # Explicitly load the specific .env file using its path
    load_dotenv(dotenv_path=temp_env_file)
    config = load_config()
    assert str(config.host) == "https://env.databricks.com/"
    assert config.token == "env_token" 