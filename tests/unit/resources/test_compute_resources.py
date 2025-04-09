import pytest
from unittest.mock import MagicMock, patch
from databricks.sdk.service import compute as compute_service

from databricks_mcp.resources.compute import list_clusters, get_cluster_details
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function used by the resources
@pytest.fixture(autouse=True)
def mock_db_client_compute():
    mock_client = MagicMock()
    # Assign unused variable to _
    with patch('databricks_mcp.resources.compute.get_db_client', return_value=mock_client) as _:
        yield mock_client # Provide the mocked client instance if needed in tests

# --- Tests for list_clusters ---

def test_list_clusters_success(mock_db_client_compute):
    # Arrange
    # Remove spec
    cluster1 = MagicMock() # spec=compute_service.ClusterDetails removed
    cluster1.cluster_id = "c1"; cluster1.cluster_name = "Cluster 1"; cluster1.state = compute_service.State.RUNNING
    cluster1.node_type_id = "type1"; cluster1.spark_version = "13.3.x"
    # Remove spec
    cluster2 = MagicMock() # spec=compute_service.ClusterDetails removed
    cluster2.cluster_id = "c2"; cluster2.cluster_name = "Cluster 2"; cluster2.state = compute_service.State.TERMINATED
    cluster2.node_type_id = "type2"; cluster2.spark_version = "12.2.x"
    # Mock the response object directly setting the 'clusters' attribute
    mock_resp = MagicMock() # spec=compute_service.ListClustersResponse removed
    mock_resp.clusters = [cluster1, cluster2]
    mock_db_client_compute.clusters.list.return_value = mock_resp

    # Act
    result = list_clusters()

    # Assert
    mock_db_client_compute.clusters.list.assert_called_once()
    assert len(result) == 2
    assert result[0]["cluster_id"] == "c1"
    assert result[0]["state"] == "RUNNING"
    assert result[1]["cluster_id"] == "c2"
    assert result[1]["state"] == "TERMINATED"

def test_list_clusters_empty(mock_db_client_compute):
    """Verify list_clusters handles an empty list."""
    mock_db_client_compute.clusters.list.return_value = []
    result = list_clusters()
    mock_db_client_compute.clusters.list.assert_called_once()
    assert result == []

# --- Tests for get_cluster_details ---

def test_get_cluster_details_success(mock_db_client_compute):
    # Arrange
    # Remove spec, set attributes directly
    cluster_info = MagicMock() # spec=compute_service.ClusterDetails removed
    cluster_info.cluster_id = "details-c1"
    cluster_info.creator_user_name = "user@example.com"
    cluster_info.cluster_name = "Detailed Cluster"
    cluster_info.spark_version = "13.3.x-scala2.12"
    cluster_info.node_type_id = "i3.xlarge"
    cluster_info.driver_node_type_id = "i3.xlarge"
    cluster_info.autotermination_minutes = 60
    cluster_info.state = compute_service.State.RUNNING
    cluster_info.state_message = "Running"
    cluster_info.autoscale = MagicMock() # Remove spec=compute_service.AutoScale
    cluster_info.autoscale.min_workers = 2
    cluster_info.autoscale.max_workers = 8
    cluster_info.num_workers = None # Ensure this is None if autoscale is used

    mock_db_client_compute.clusters.get.return_value = cluster_info

    # Act
    result = get_cluster_details(cluster_id="details-c1")

    # Assert
    mock_db_client_compute.clusters.get.assert_called_once_with(cluster_id="details-c1")
    assert result["cluster_id"] == "details-c1"
    assert result["cluster_name"] == "Detailed Cluster"
    assert result["state"] == "RUNNING"
    assert result["autoscale"] == {"min_workers": 2, "max_workers": 8}
    assert "num_workers" not in result # Check fixed size key is not present

def test_get_cluster_details_fixed_size(mock_db_client_compute):
    # Arrange
    # Remove spec, set attributes directly
    cluster_info_fixed = MagicMock() # spec=compute_service.ClusterDetails removed
    cluster_info_fixed.cluster_id = "fixed-c1"
    cluster_info_fixed.creator_user_name = "user@example.com"
    cluster_info_fixed.cluster_name = "Fixed Size Cluster"
    cluster_info_fixed.spark_version = "13.3.x-scala2.12"
    cluster_info_fixed.node_type_id = "m5.large"
    cluster_info_fixed.driver_node_type_id = "m5.large"
    cluster_info_fixed.autotermination_minutes = 0
    cluster_info_fixed.state = compute_service.State.TERMINATED
    cluster_info_fixed.state_message = "Terminated by user"
    cluster_info_fixed.autoscale = None # Ensure autoscale is None
    cluster_info_fixed.num_workers = 5

    mock_db_client_compute.clusters.get.return_value = cluster_info_fixed

    # Act
    result = get_cluster_details(cluster_id="fixed-c1")

    # Assert
    mock_db_client_compute.clusters.get.assert_called_once_with(cluster_id="fixed-c1")
    assert result["cluster_id"] == "fixed-c1"
    assert result["state"] == "TERMINATED"
    assert result["num_workers"] == 5
    assert "autoscale" not in result # Check autoscale key is not present

# We rely on the map_databricks_errors decorator to handle exceptions,
# so we don't explicitly test raising MCPError here unless we want to test
# the decorator itself (which might be done separately).
# Testing the *presence* of the decorator is implicit.
