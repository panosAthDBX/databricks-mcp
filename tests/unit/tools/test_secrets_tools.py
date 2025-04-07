import base64
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import secrets as secrets_service
from mcp import errors as mcp_errors

from databricks_mcp.tools.secrets import delete_secret
from databricks_mcp.tools.secrets import get_secret
from databricks_mcp.tools.secrets import put_secret
from databricks_mcp.db_client import get_db_client # To mock


# Mock settings for conditional check
@pytest.fixture
def mock_settings():
    mock = MagicMock()
    # Default to enabled for most tests, override when needed
    mock.enable_get_secret = True
    with patch('databricks_mcp.tools.secrets.settings', mock):
        yield mock

# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_secrets_tools():
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.tools.secrets.get_db_client', return_value=mock_client) as _:
        yield mock_client


# --- Tests for get_secret ---
def test_get_secret_success_string(mock_settings, mock_db_client_secrets_tools):
    # Arrange
    scope = "scope1"
    key = "key1"
    value_bytes = b"my_secret_value"
    mock_resp = MagicMock(spec=secrets_service.GetSecretResponse)
    mock_resp.value = value_bytes
    mock_db_client_secrets_tools.secrets.get_secret.return_value = mock_resp
    mock_settings.enable_get_secret = True # Ensure enabled

    # Act
    result = get_secret(scope_name=scope, key=key)

    # Assert
    mock_db_client_secrets_tools.secrets.get_secret.assert_called_once_with(scope=scope, key=key)
    assert result["scope"] == scope
    assert result["key"] == key
    assert result["value_string"] == "my_secret_value"
    assert result["value_base64"] is None

def test_get_secret_success_bytes(mock_settings, mock_db_client_secrets_tools):
    # Arrange
    scope = "scope_bin"
    key = "key_bin"
    value_bytes = b'\x01\x02\xff\xfe' # Non-utf8 bytes
    encoded_value = base64.b64encode(value_bytes).decode('ascii')
    mock_resp = MagicMock(spec=secrets_service.GetSecretResponse)
    mock_resp.value = value_bytes
    mock_db_client_secrets_tools.secrets.get_secret.return_value = mock_resp
    mock_settings.enable_get_secret = True

    # Act
    result = get_secret(scope_name=scope, key=key)

    # Assert
    mock_db_client_secrets_tools.secrets.get_secret.assert_called_once_with(scope=scope, key=key)
    assert result["scope"] == scope
    assert result["key"] == key
    assert result["value_string"] is None
    assert result["value_base64"] == encoded_value


def test_get_secret_disabled(mock_settings, mock_db_client_secrets_tools):
    # Arrange
    mock_settings.enable_get_secret = False # Disable the tool via config mock

    # Act & Assert
    with pytest.raises(mcp_errors.MCPError) as exc_info:
        get_secret(scope_name="scope", key="key")

    assert exc_info.value.code == mcp_errors.ErrorCode.SERVER_ERROR_PERMISSION_DENIED
    assert "disabled by server configuration" in exc_info.value.message
    mock_db_client_secrets_tools.secrets.get_secret.assert_not_called()

# --- Tests for put_secret ---
def test_put_secret_success(mock_db_client_secrets_tools):
    # Arrange
    scope = "scope_put"
    key = "new_key"
    value = "new_value"

    # Act
    result = put_secret(scope_name=scope, key=key, secret_value=value)

    # Assert
    mock_db_client_secrets_tools.secrets.put_secret.assert_called_once_with(scope=scope, key=key, string_value=value)
    assert result == {"scope": scope, "key": key, "status": "SUCCESS"}

# --- Tests for delete_secret ---
def test_delete_secret_success(mock_db_client_secrets_tools):
     # Arrange
    scope = "scope_del"
    key = "del_key"

    # Act
    result = delete_secret(scope_name=scope, key=key)

    # Assert
    mock_db_client_secrets_tools.secrets.delete_secret.assert_called_once_with(scope=scope, key=key)
    assert result == {"scope": scope, "key": key, "status": "SUCCESS"}

# Add tests for SDK error mapping if needed
