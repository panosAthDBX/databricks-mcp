import base64
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import repos as repos_service
from databricks.sdk.service import workspace as workspace_service

from databricks_mcp.resources.workspace import get_notebook_content
from databricks_mcp.resources.workspace import get_repo_status
from databricks_mcp.resources.workspace import list_repos
from databricks_mcp.resources.workspace import list_workspace_items
from databricks_mcp.db_client import get_db_client # To mock


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_ws(): # Changed fixture name slightly to avoid potential clashes
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.resources.workspace.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for list_workspace_items ---

def test_list_workspace_items_success(mock_db_client_ws):
    # Arrange
    item1 = MagicMock(spec=workspace_service.ObjectInfo)
    item1.path = "/Users/test/notebook"
    item1.object_type = workspace_service.ObjectType.NOTEBOOK
    item1.object_id = 123

    item2 = MagicMock(spec=workspace_service.ObjectInfo)
    item2.path = "/Users/test/folder"
    item2.object_type = workspace_service.ObjectType.DIRECTORY
    item2.object_id = 456

    mock_db_client_ws.workspace.list.return_value = [item1, item2]

    # Act
    result = list_workspace_items(path="/Users/test")

    # Assert
    mock_db_client_ws.workspace.list.assert_called_once_with(path="/Users/test")
    assert len(result) == 2
    assert result[0] == {"path": "/Users/test/notebook", "type": "NOTEBOOK", "object_id": 123}
    assert result[1] == {"path": "/Users/test/folder", "type": "DIRECTORY", "object_id": 456}

# --- Tests for get_notebook_content ---

def test_get_notebook_content_success(mock_db_client_ws):
    # Arrange
    notebook_path = "/Users/test/my_notebook"
    raw_content = "# Databricks notebook source\nprint('hello')"
    encoded_content = base64.b64encode(raw_content.encode('utf-8')).decode('ascii')

    mock_export = MagicMock(spec=workspace_service.ExportResponse)
    mock_export.content = encoded_content

    mock_status = MagicMock(spec=workspace_service.ObjectInfo)
    mock_status.language = workspace_service.Language.PYTHON

    mock_db_client_ws.workspace.export.return_value = mock_export
    mock_db_client_ws.workspace.get_status.return_value = mock_status

    # Act
    result = get_notebook_content(path=notebook_path)

    # Assert
    mock_db_client_ws.workspace.export.assert_called_once_with(path=notebook_path, format=workspace_service.ExportFormat.SOURCE)
    mock_db_client_ws.workspace.get_status.assert_called_once_with(path=notebook_path)
    assert result["path"] == notebook_path
    assert result["content"] == raw_content
    assert result["language"] == "PYTHON"

def test_get_notebook_content_decode_error(mock_db_client_ws):
     # Arrange
    notebook_path = "/Users/test/bad_notebook"
    bad_encoded_content = "this is not base64" # Invalid base64

    mock_export = MagicMock(spec=workspace_service.ExportResponse)
    mock_export.content = bad_encoded_content

    mock_status = MagicMock(spec=workspace_service.ObjectInfo)
    mock_status.language = None # No language info

    mock_db_client_ws.workspace.export.return_value = mock_export
    mock_db_client_ws.workspace.get_status.return_value = mock_status # Or mock it to raise

    # Act
    result = get_notebook_content(path=notebook_path)

    # Assert
    assert result["path"] == notebook_path
    assert "Error: Failed to decode content." in result["content"]
    assert result["language"] == "UNKNOWN"


# --- Tests for list_repos ---

def test_list_repos_success(mock_db_client_ws):
    # Arrange
    repo1 = MagicMock(spec=repos_service.RepoInfo)
    repo1.id = "repo1"
    repo1.path = "/Repos/test/repo-one"
    repo1.url = "git@github.com:user/repo-one.git"
    repo1.branch = "main"
    repo1.head_commit_id = "abc123def"

    mock_db_client_ws.repos.list.return_value = [repo1]

    # Act
    result = list_repos()

    # Assert
    mock_db_client_ws.repos.list.assert_called_once()
    assert len(result) == 1
    assert result[0] == {
        "id": "repo1",
        "path": "/Repos/test/repo-one",
        "url": "git@github.com:user/repo-one.git",
        "branch": "main",
        "head_commit_id": "abc123def",
    }

# --- Tests for get_repo_status ---

def test_get_repo_status_success(mock_db_client_ws):
     # Arrange
    repo_id_to_get = "repo-xyz"
    mock_repo_info = MagicMock(spec=repos_service.RepoInfo)
    mock_repo_info.id = repo_id_to_get
    mock_repo_info.url = "git@github.com:org/repo-xyz.git"
    mock_repo_info.branch = "develop"
    mock_repo_info.head_commit_id = "fed456cba"

    mock_db_client_ws.repos.get.return_value = mock_repo_info

    # Act
    result = get_repo_status(repo_id=repo_id_to_get)

    # Assert
    mock_db_client_ws.repos.get.assert_called_once_with(repo_id=repo_id_to_get)
    assert result == {
        "repo_id": repo_id_to_get,
        "url": "git@github.com:org/repo-xyz.git",
        "branch": "develop",
        "head_commit_id": "fed456cba",
    }
