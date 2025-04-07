import structlog
from mcp import Parameter
from mcp import Tool
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Tool.from_callable(
    "databricks:compute:start_cluster",
    description="Starts a terminated Databricks cluster.",
    parameters=[
        Parameter(
            name="cluster_id",
            description="The unique identifier of the cluster to start.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def start_cluster(cluster_id: str) -> dict:
    """
    Starts a terminated cluster.
    REQ-COMP-TOOL-01
    Returns a dictionary indicating the action status.
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
@Tool.from_callable(
    "databricks:compute:terminate_cluster",
    description="Terminates a running Databricks cluster.",
    parameters=[
        Parameter(
            name="cluster_id",
            description="The unique identifier of the cluster to terminate.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def terminate_cluster(cluster_id: str) -> dict:
    """
    Terminates a running cluster.
    REQ-COMP-TOOL-02
    Returns a dictionary indicating the action status.
    """
    db = get_db_client()
    log.info("Terminating Databricks cluster", cluster_id=cluster_id)
    # The delete() method (terminate) is synchronous and waits for TERMINATED state.
    db.clusters.delete(cluster_id=cluster_id).result() # Use .result() to wait and raise errors
    status = "TERMINATED" # Assuming success if no exception
    log.info("Successfully terminated cluster", cluster_id=cluster_id, status=status)
    return {"cluster_id": cluster_id, "status": status}
