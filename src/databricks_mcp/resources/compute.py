import structlog
from mcp import Parameter
from mcp import Resource
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Resource.from_callable(
    "databricks:compute:list_clusters",
    description="Lists all available Databricks clusters in the workspace.",
    parameters=[], # No parameters needed for listing all clusters
)
def list_clusters() -> list[dict]:
    """
    Lists available clusters and their states.
    REQ-COMP-RES-01
    """
    db = get_db_client()
    log.info("Listing Databricks clusters")
    clusters = db.clusters.list() # This yields ClusterInfo objects

    # Format the output as specified roughly in the PRD
    result = [
        {
            "cluster_id": c.cluster_id,
            "name": c.cluster_name,
            "state": str(c.state.value) if c.state else "UNKNOWN", # Convert enum to string
            "driver_node_type": c.driver_node_type_id,
            "worker_node_type": c.node_type_id, # Note: node_type_id is for workers when not autoscaling
            # Add more fields if desired, matching ClusterInfo attributes
        }
        for c in clusters
    ]
    log.info("Successfully listed clusters", count=len(result))
    return result

@map_databricks_errors
@Resource.from_callable(
    "databricks:compute:get_cluster_details",
    description="Gets detailed information about a specific Databricks cluster.",
    parameters=[
        Parameter(
            name="cluster_id",
            description="The unique identifier of the cluster.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def get_cluster_details(cluster_id: str) -> dict:
    """
    Gets detailed information about a specific cluster.
    REQ-COMP-RES-02
    """
    db = get_db_client()
    log.info("Getting details for Databricks cluster", cluster_id=cluster_id)
    cluster_info = db.clusters.get(cluster_id=cluster_id)

    # Convert the ClusterInfo object to a dictionary for MCP serialization.
    # We might need a more robust way to handle nested objects/enums if needed.
    # For now, converting basic fields and state enum.
    details = {
        "cluster_id": cluster_info.cluster_id,
        "creator_user_name": cluster_info.creator_user_name,
        "cluster_name": cluster_info.cluster_name,
        "spark_version": cluster_info.spark_version,
        "node_type_id": cluster_info.node_type_id,
        "driver_node_type_id": cluster_info.driver_node_type_id,
        "autotermination_minutes": cluster_info.autotermination_minutes,
        "state": str(cluster_info.state.value) if cluster_info.state else "UNKNOWN",
        "state_message": cluster_info.state_message,
        # Add other relevant fields from ClusterInfo as needed
    }
    if cluster_info.autoscale:
        details["autoscale"] = {
            "min_workers": cluster_info.autoscale.min_workers,
            "max_workers": cluster_info.autoscale.max_workers,
        }
    else:
         details["num_workers"] = cluster_info.num_workers

    log.info("Successfully retrieved cluster details", cluster_id=cluster_id)
    return details
