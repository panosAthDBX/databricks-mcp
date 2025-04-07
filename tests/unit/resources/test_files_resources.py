from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import dbfs as dbfs_service

from databricks_mcp.resources.files import list_files


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_files():
    mock_client = MagicMock()
    with patch('databricks_mcp.resources.files.get_db_client', return_value=mock_client) as mock_get:
        yield mock_client

# --- Tests for list_files ---

@pytest.mark.parametrize("path_to_list", ["/dbfs/data", "/mnt/my_mount", "/other/path"])
def test_list_files_dbfs_paths(mock_db_client_files, path_to_list):
    # Arrange
    file_info = MagicMock(spec=dbfs_service.FileInfo)
    file_info.path = f"{path_to_list.rstrip('/')}/file.txt"
    file_info.is_dir = False
    file_info.file_size = 1024

    dir_info = MagicMock(spec=dbfs_service.FileInfo)
    dir_info.path = f"{path_to_list.rstrip('/')}/subdir"
    dir_info.is_dir = True
    dir_info.file_size = 0 # Dirs usually have 0 size

    mock_db_client_files.dbfs.list.return_value = [file_info, dir_info]

    # Act
    result = list_files(path=path_to_list)

    # Assert
    mock_db_client_files.dbfs.list.assert_called_once_with(path=path_to_list)
    assert len(result) == 2
    assert result[0] == {"path": f"{path_to_list.rstrip('/')}/file.txt", "is_dir": False, "size": 1024}
    assert result[1] == {"path": f"{path_to_list.rstrip('/')}/subdir", "is_dir": True, "size": 0}

def test_list_files_volume_path_uses_dbfs_api(mock_db_client_files):
    """Verify (for now) that Volume paths also call the dbfs.list mock"""
     # Arrange
    volume_path = "/Volumes/main/default/myvol"
    file_info = MagicMock(spec=dbfs_service.FileInfo)
    file_info.path = f"{volume_path}/vol_file.csv"
    file_info.is_dir = False
    file_info.file_size = 500
    mock_db_client_files.dbfs.list.return_value = [file_info]

    # Act
    result = list_files(path=volume_path)

    # Assert
    mock_db_client_files.dbfs.list.assert_called_once_with(path=volume_path)
    assert len(result) == 1
    assert result[0]["path"] == f"{volume_path}/vol_file.csv"
