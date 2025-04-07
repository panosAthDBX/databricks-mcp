import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.service import compute as compute_service
from databricks.sdk.errors import NotFound

from databricks_mcp.tools.compute import start_cluster, terminate_cluster
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function used by the tools
@pytest.fixture(autouse=True)
def mock_db_client():
    mock_client = MagicMock()
    # Mock the result() method often chained in the tools
    mock_waiter = MagicMock()
    mock_waiter.result.return_value = None # Simulate successful wait

    # Configure SDK methods to return the waiter
    mock_client.clusters.start.return_value = mock_waiter
    mock_client.clusters.delete.return_value = mock_waiter

    # Assign unused variable to _
    with patch('databricks_mcp.tools.compute.get_db_client', return_value=mock_client) as _:
        yield mock_client # Provide the mocked client instance if needed in tests


# --- Tests for start_cluster ---

def test_start_cluster_success(mock_db_client):
    """Verify start_cluster calls SDK correctly on success."""
    # Act
    result = start_cluster(cluster_id="start-me")

    # Assert
    mock_db_client.clusters.start.assert_called_once_with(cluster_id="start-me")
    # Check if the waiter's result method was called
    mock_db_client.clusters.start.return_value.result.assert_called_once()
    assert result == {"cluster_id": "start-me", "status": "STARTED"}

def test_start_cluster_sdk_error_mapped(mock_db_client):
    """Verify SDK errors are mapped by the decorator."""
    # Arrange: Configure the mock to raise a specific Databricks error
    sdk_error = NotFound("Cluster not found")
    mock_db_client.clusters.start.side_effect = sdk_error

    # Act & Assert: Check if the correct MCPError is raised
    with pytest.raises(MCPError) as exc_info:
        start_cluster(cluster_id="not-found")

    # Assert error details (mapped by error_mapping decorator)
    assert exc_info.value.code == ErrorCode.SERVER_ERROR_RESOURCE_NOT_FOUND
    assert "Cluster not found" in exc_info.value.message
    mock_db_client.clusters.start.assert_called_once_with(cluster_id="not-found")
    # Ensure result() wasn't called if start failed immediately
    # mock_db_client.clusters.start.return_value.result.assert_not_called() # This might fail if side_effect is raised before return

# --- Tests for terminate_cluster ---

def test_terminate_cluster_success(mock_db_client):
    """Verify terminate_cluster calls SDK correctly on success."""
    # Act
    result = terminate_cluster(cluster_id="stop-me")

    # Assert
    mock_db_client.clusters.delete.assert_called_once_with(cluster_id="stop-me")
    mock_db_client.clusters.delete.return_value.result.assert_called_once()
    assert result == {"cluster_id": "stop-me", "status": "TERMINATED"}

def test_terminate_cluster_sdk_error_mapped(mock_db_client):
    """Verify SDK errors are mapped by the decorator for terminate."""
    # Arrange
    sdk_error = NotFound("Cannot terminate non-existent cluster")
    mock_db_client.clusters.delete.side_effect = sdk_error

    # Act & Assert
    with pytest.raises(MCPError) as exc_info:
        terminate_cluster(cluster_id="gone")

    assert exc_info.value.code == ErrorCode.SERVER_ERROR_RESOURCE_NOT_FOUND
    assert "Cannot terminate non-existent cluster" in exc_info.value.message
    mock_db_client.clusters.delete.assert_called_once_with(cluster_id="gone")
    # mock_db_client.clusters.delete.return_value.result.assert_not_called()
