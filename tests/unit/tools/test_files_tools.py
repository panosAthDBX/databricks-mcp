import pytest
import base64
from unittest.mock import MagicMock, patch
from databricks.sdk.service import files as dbfs_service
from databricks.sdk.errors import DatabricksError # Import general error
# Import error code constant
from databricks_mcp.error_mapping import CODE_SERVER_ERROR

from databricks_mcp.tools.files import (
    read_file,
    write_file,
    delete_file,
    create_directory
)
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_files_tools():
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.tools.files.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for read_file ---
def test_read_file_success(mock_db_client_files_tools):
    # Arrange
    path = "/dbfs/test.txt"
    raw_content = "Hello MCP!"
    encoded_content = base64.b64encode(raw_content.encode('utf-8')).decode('ascii')
    read_resp = MagicMock(spec=dbfs_service.ReadResponse)
    read_resp.data = encoded_content
    read_resp.bytes_read = len(raw_content)
    mock_db_client_files_tools.dbfs.read.return_value = read_resp

    # Act
    result = read_file(path=path, length=100) # Specify length

    # Assert
    # Read max 1MB if length=0, otherwise use specified length
    expected_read_length = 100
    mock_db_client_files_tools.dbfs.read.assert_called_once_with(path=path, offset=0, length=expected_read_length)
    assert result == {"path": path, "content_base64": encoded_content, "bytes_read": len(raw_content)}

def test_read_file_default_length(mock_db_client_files_tools):
     # Arrange
    path = "/dbfs/default_len.txt"
    read_resp = MagicMock(spec=dbfs_service.ReadResponse)
    read_resp.data = ""
    read_resp.bytes_read = 0
    mock_db_client_files_tools.dbfs.read.return_value = read_resp
    # Act
    read_file(path=path) # No length specified
    # Assert - should use default max read size (1MB)
    mock_db_client_files_tools.dbfs.read.assert_called_once_with(path=path, offset=0, length=1024*1024)


# --- Tests for write_file ---
def test_write_file_success(mock_db_client_files_tools):
    # Arrange
    path = "/dbfs/new_file.log"
    raw_content = "Log entry"
    encoded_content = base64.b64encode(raw_content.encode('utf-8')).decode('ascii')
    mock_handle = 12345
    mock_db_client_files_tools.dbfs.create.return_value = MagicMock(handle=mock_handle)

    # Act
    result = write_file(path=path, content_base64=encoded_content, overwrite=True)

    # Assert
    mock_db_client_files_tools.dbfs.create.assert_called_once_with(path=path, overwrite=True)
    mock_db_client_files_tools.dbfs.add_block.assert_called_once_with(handle=mock_handle, data=encoded_content)
    mock_db_client_files_tools.dbfs.close.assert_called_once_with(handle=mock_handle)
    assert result == {"path": path, "status": "SUCCESS", "bytes_written": len(raw_content)}

def test_write_file_sdk_error_during_write(mock_db_client_files_tools):
     # Arrange
    path = "/dbfs/fail_write.log"
    encoded_content = "YWFh" # "aaa"
    mock_handle = 6789
    mock_db_client_files_tools.dbfs.create.return_value = MagicMock(handle=mock_handle)
    # Simulate error during add_block
    sdk_error = DatabricksError("Disk full") # Use general error
    mock_db_client_files_tools.dbfs.add_block.side_effect = sdk_error

    # Act & Assert - Expect the mapped generic Exception
    # with pytest.raises(DatabricksError, match="Disk full"): # OLD
    with pytest.raises(Exception) as exc_info:
         write_file(path=path, content_base64=encoded_content)

    # Assert the wrapped exception message
    assert f"[MCP Error Code {CODE_SERVER_ERROR}]" in str(exc_info.value)
    assert "DatabricksError" in str(exc_info.value)
    assert "Disk full" in str(exc_info.value)

    # Ensure create was called, add_block was attempted, close was not
    mock_db_client_files_tools.dbfs.create.assert_called_once_with(path=path, overwrite=False)
    mock_db_client_files_tools.dbfs.add_block.assert_called_once_with(handle=mock_handle, data=encoded_content)
    mock_db_client_files_tools.dbfs.close.assert_not_called()

# --- Tests for delete_file ---
def test_delete_file_success(mock_db_client_files_tools):
    path = "/dbfs/to_delete.tmp"
    result = delete_file(path=path, recursive=False)
    mock_db_client_files_tools.dbfs.delete.assert_called_once_with(path=path, recursive=False)
    assert result == {"path": path, "status": "SUCCESS"}

def test_delete_file_recursive(mock_db_client_files_tools):
    path = "/dbfs/dir_to_delete"
    result = delete_file(path=path, recursive=True)
    mock_db_client_files_tools.dbfs.delete.assert_called_once_with(path=path, recursive=True)
    assert result == {"path": path, "status": "SUCCESS"}

# --- Tests for create_directory ---
def test_create_directory_success(mock_db_client_files_tools):
    path = "/dbfs/new/nested/dir"
    result = create_directory(path=path)
    mock_db_client_files_tools.dbfs.mkdirs.assert_called_once_with(path=path)
    assert result == {"path": path, "status": "SUCCESS"}
