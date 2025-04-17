import structlog
# Import the mcp instance from app.py
from ..app import mcp

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
# Use the mcp instance decorator
@mcp.tool(
    name="databricks-compute-start_cluster",
    description="Starts a terminated Databricks cluster.",
)
def start_cluster(cluster_id: str) -> dict:
    """
    Starts a terminated cluster.
    REQ-COMP-TOOL-01
    Returns a dictionary indicating the action status.

    Args:
        cluster_id: The unique identifier of the cluster to start.
    """
    db = get_db_client()
    log.info("Starting Databricks cluster", cluster_id=cluster_id)
    # The start() method is synchronous and waits for the cluster to be RUNNING
    # or encounters an error.
    db.clusters.start(cluster_id=cluster_id).result() # Use .result() to wait and raise errors
    status = "STARTED" # Assuming success if no exception
    log.info("Successfully started cluster", cluster_id=cluster_id, status=status)
    return {"cluster_id": cluster_id, "status": status}


@map_databricks_errors
# Use the mcp instance decorator
@mcp.tool(
    name="databricks-compute-terminate_cluster",
    description="Terminates a running Databricks cluster.",
)
def terminate_cluster(cluster_id: str) -> dict:
    """
    Terminates a running cluster.
    REQ-COMP-TOOL-02
    Returns a dictionary indicating the action status.

    Args:
        cluster_id: The unique identifier of the cluster to terminate.
    """
    db = get_db_client()
    log.info("Terminating Databricks cluster", cluster_id=cluster_id)
    # The delete() method (terminate) is synchronous and waits for TERMINATED state.
    db.clusters.delete(cluster_id=cluster_id).result() # Use .result() to wait and raise errors
    status = "TERMINATED" # Assuming success if no exception
    log.info("Successfully terminated cluster", cluster_id=cluster_id, status=status)
    return {"cluster_id": cluster_id, "status": status}
