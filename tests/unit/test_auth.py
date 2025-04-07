import pytest
from unittest.mock import patch, MagicMock

from databricks.sdk import WorkspaceClient
from databricks_mcp.config import DatabricksConfig
from databricks_mcp.auth import get_authenticated_client

# Fixture for a valid config
@pytest.fixture
def valid_config():
    return DatabricksConfig(host="https://test.databricks.com", token="test_token")

# Fixture for an invalid config (missing token)
@pytest.fixture
def invalid_config():
    # This fixture is problematic as Pydantic prevents creating this directly.
    # The test using it will be modified.
    # We keep the fixture definition simple for now, but it won't be used.
    pass

# Test successful authentication
@patch('databricks_mcp.auth.WorkspaceClient')
def test_get_authenticated_client_success(mock_workspace_client, valid_config):
    # Mock the WorkspaceClient instance and its me() method
    mock_client_instance = MagicMock()
    # Use a generic MagicMock for the user object returned by me()
    mock_user = MagicMock()
    mock_user.user_name = "test@example.com"
    mock_client_instance.current_user.me.return_value = mock_user
    mock_workspace_client.return_value = mock_client_instance

    client = get_authenticated_client(valid_config)

    # Assertions
    mock_workspace_client.assert_called_once_with(host="https://test.databricks.com/", token="test_token")
    mock_client_instance.current_user.me.assert_called_once()
    assert client == mock_client_instance

# Test authentication check failure
@patch('databricks_mcp.auth.WorkspaceClient')
def test_get_authenticated_client_auth_check_fails(mock_workspace_client, valid_config):
    # Mock the WorkspaceClient instance but make me() raise an error
    mock_client_instance = MagicMock()
    mock_client_instance.current_user.me.side_effect = ConnectionError("Auth failed")
    mock_workspace_client.return_value = mock_client_instance

    with pytest.raises(ConnectionError, match="Failed to verify authentication with Databricks: Auth failed"):
        get_authenticated_client(valid_config)

    mock_workspace_client.assert_called_once_with(host="https://test.databricks.com/", token="test_token")
    mock_client_instance.current_user.me.assert_called_once()

# Test failure during WorkspaceClient initialization
@patch('databricks_mcp.auth.WorkspaceClient', side_effect=Exception("Init failed"))
def test_get_authenticated_client_init_fails(mock_workspace_client, valid_config):
    with pytest.raises(ConnectionError, match="Failed to initialize Databricks client: Init failed"):
        get_authenticated_client(valid_config)

    mock_workspace_client.assert_called_once_with(host="https://test.databricks.com/", token="test_token")

# Test invalid configuration provided (e.g., missing token)
def test_get_authenticated_client_invalid_config():
    # Create a config object directly that violates the requirements for the function
    # (even if Pydantic wouldn't allow creating it with token=None normally)
    # We can mock it or construct it carefully for the test's purpose.
    # Option 1: Mock the config object passed to the function
    mock_config = MagicMock(spec=DatabricksConfig)
    mock_config.host = "https://test.databricks.com"
    mock_config.token = None # Simulate missing token

    with pytest.raises(ValueError, match="Databricks host and token must be provided"):
        get_authenticated_client(mock_config)

    # Option 2 (Alternative if mocking is difficult): 
    # If the function only checks attributes, create a simple object
    # class SimpleConfig:
    #     host = "https://test.databricks.com"
    #     token = None
    # with pytest.raises(ValueError, match="Databricks host and token must be provided"):
    #     get_authenticated_client(SimpleConfig()) 