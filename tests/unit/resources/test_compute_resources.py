import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.service import compute as compute_service

from databricks_mcp.resources.compute import list_clusters, get_cluster_details
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function used by the resources
@pytest.fixture(autouse=True)
def mock_db_client():
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.resources.compute.get_db_client', return_value=mock_client) as _:
        yield mock_client # Provide the mocked client instance if needed in tests

# --- Tests for list_clusters ---

def test_list_clusters_success(mock_db_client):
    """Verify list_clusters returns formatted cluster list."""
    # Arrange: Mock the SDK response
    mock_cluster_1 = MagicMock(spec=compute_service.ClusterDetails)
    mock_cluster_1.cluster_id = "c1-id"
    mock_cluster_1.cluster_name = "Cluster One"
    mock_cluster_1.state = compute_service.ClusterState.RUNNING
    mock_cluster_1.driver_node_type_id = "driver-type"
    mock_cluster_1.node_type_id = "worker-type"

    mock_cluster_2 = MagicMock(spec=compute_service.ClusterDetails)
    mock_cluster_2.cluster_id = "c2-id"
    mock_cluster_2.cluster_name = "Cluster Two"
    mock_cluster_2.state = compute_service.ClusterState.TERMINATED
    mock_cluster_2.driver_node_type_id = "driver-type-2"
    mock_cluster_2.node_type_id = "worker-type-2"

    mock_db_client.clusters.list.return_value = [mock_cluster_1, mock_cluster_2]

    # Act
    result = list_clusters()

    # Assert
    mock_db_client.clusters.list.assert_called_once()
    assert len(result) == 2
    assert result[0] == {
        "cluster_id": "c1-id",
        "name": "Cluster One",
        "state": "RUNNING",
        "driver_node_type": "driver-type",
        "worker_node_type": "worker-type",
    }
    assert result[1]["cluster_id"] == "c2-id"
    assert result[1]["state"] == "TERMINATED"

def test_list_clusters_empty(mock_db_client):
    """Verify list_clusters handles an empty list."""
    mock_db_client.clusters.list.return_value = []
    result = list_clusters()
    mock_db_client.clusters.list.assert_called_once()
    assert result == []

# --- Tests for get_cluster_details ---

def test_get_cluster_details_success(mock_db_client):
    """Verify get_cluster_details returns formatted details."""
    # Arrange
    mock_cluster_info = MagicMock(spec=compute_service.ClusterDetails)
    mock_cluster_info.cluster_id = "test-id"
    mock_cluster_info.creator_user_name = "user@example.com"
    mock_cluster_info.cluster_name = "Test Cluster"
    mock_cluster_info.spark_version = "13.3.x-scala2.12"
    mock_cluster_info.node_type_id = "worker-type"
    mock_cluster_info.driver_node_type_id = "driver-type"
    mock_cluster_info.autotermination_minutes = 60
    mock_cluster_info.state = compute_service.ClusterState.RUNNING
    mock_cluster_info.state_message = "Running smoothly"
    mock_cluster_info.autoscale = compute_service.AutoScale(min_workers=1, max_workers=5)
    mock_cluster_info.num_workers = None # Autoscaling enabled

    mock_db_client.clusters.get.return_value = mock_cluster_info

    # Act
    result = get_cluster_details(cluster_id="test-id")

    # Assert
    mock_db_client.clusters.get.assert_called_once_with(cluster_id="test-id")
    assert result["cluster_id"] == "test-id"
    assert result["cluster_name"] == "Test Cluster"
    assert result["state"] == "RUNNING"
    assert "autoscale" in result
    assert result["autoscale"]["min_workers"] == 1
    assert result["autoscale"]["max_workers"] == 5
    assert "num_workers" not in result

def test_get_cluster_details_fixed_size(mock_db_client):
    """Verify get_cluster_details handles fixed size clusters."""
    # Arrange
    mock_cluster_info = MagicMock(spec=compute_service.ClusterDetails)
    mock_cluster_info.cluster_id = "fixed-id"
    mock_cluster_info.state = compute_service.ClusterState.PENDING
    mock_cluster_info.autoscale = None # Fixed size
    mock_cluster_info.num_workers = 3

    mock_db_client.clusters.get.return_value = mock_cluster_info

    # Act
    result = get_cluster_details(cluster_id="fixed-id")

    # Assert
    mock_db_client.clusters.get.assert_called_once_with(cluster_id="fixed-id")
    assert result["cluster_id"] == "fixed-id"
    assert result["state"] == "PENDING"
    assert "autoscale" not in result
    assert "num_workers" in result
    assert result["num_workers"] == 3

# We rely on the map_databricks_errors decorator to handle exceptions,
# so we don't explicitly test raising MCPError here unless we want to test
# the decorator itself (which might be done separately).
# Testing the *presence* of the decorator is implicit.
