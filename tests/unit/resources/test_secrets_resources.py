import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.service import secrets as secrets_service

from databricks_mcp.resources.secrets import list_secret_scopes, list_secrets
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_secrets():
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.resources.secrets.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for list_secret_scopes ---
def test_list_secret_scopes_success(mock_db_client_secrets):
    # Arrange
    scope1 = MagicMock(spec=secrets_service.SecretScope)
    scope1.name = "scope1"
    scope2 = MagicMock(spec=secrets_service.SecretScope)
    scope2.name = "scope2-kv"
    mock_resp = MagicMock(spec=secrets_service.ListScopesResponse)
    mock_resp.scopes = [scope1, scope2]
    mock_db_client_secrets.secrets.list_scopes.return_value = mock_resp

    # Act
    result = list_secret_scopes()

    # Assert
    mock_db_client_secrets.secrets.list_scopes.assert_called_once()
    assert len(result) == 2
    assert result[0] == {"name": "scope1"}
    assert result[1] == {"name": "scope2-kv"}

def test_list_secret_scopes_empty(mock_db_client_secrets):
    mock_resp = MagicMock(spec=secrets_service.ListScopesResponse)
    mock_resp.scopes = []
    mock_db_client_secrets.secrets.list_scopes.return_value = mock_resp
    result = list_secret_scopes()
    assert result == []

# --- Tests for list_secrets ---
def test_list_secrets_success(mock_db_client_secrets):
    # Arrange
    secret1 = MagicMock(spec=secrets_service.SecretMetadata)
    secret1.key = "key1"
    secret1.last_updated_timestamp = 1234567890000
    secret2 = MagicMock(spec=secrets_service.SecretMetadata)
    secret2.key = "another-key"
    secret2.last_updated_timestamp = 1234567990000
    mock_resp = MagicMock(spec=secrets_service.ListSecretsResponse)
    mock_resp.secrets = [secret1, secret2]
    mock_db_client_secrets.secrets.list_secrets.return_value = mock_resp

    # Act
    result = list_secrets(scope_name="my_scope")

    # Assert
    mock_db_client_secrets.secrets.list_secrets.assert_called_once_with(scope="my_scope")
    assert len(result) == 2
    assert result[0] == {"key": "key1", "last_updated_timestamp": 1234567890000}
    assert result[1] == {"key": "another-key", "last_updated_timestamp": 1234567990000}

def test_list_secrets_empty(mock_db_client_secrets):
    mock_resp = MagicMock(spec=secrets_service.ListSecretsResponse)
    mock_resp.secrets = []
    mock_db_client_secrets.secrets.list_secrets.return_value = mock_resp
    result = list_secrets(scope_name="empty_scope")
    assert result == []
