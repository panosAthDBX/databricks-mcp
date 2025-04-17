import structlog
# Import the mcp instance from app.py
from ..app import mcp

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors
from databricks.sdk.service import jobs as jobs_service

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.tool(
    name="databricks-jobs-run_now",
    description=(
        "Triggers a specific Databricks Job to run immediately and waits for its completion. "
        "Optional parameters can be provided to override job settings for this run. "
        "NOTE: This tool currently blocks until the job run finishes, fails, or times out."
    ),
)
def run_job_now(
    job_id: int,
    notebook_params: dict | None = None,
    python_params: list[str] | None = None,
    jar_params: list[str] | None = None,
    spark_submit_params: list[str] | None = None,
) -> dict:
    """
    Triggers a specific job to run immediately and waits for completion.
    REQ-JOB-TOOL-01

    Args:
        job_id: The unique identifier of the job to run.
        notebook_params: Optional dictionary of notebook parameters to override.
        python_params: Optional list of string arguments for a Python script task.
        jar_params: Optional list of string arguments for a JAR task.
        spark_submit_params: Optional list of string arguments for a Spark submit task.
    """
    db = get_db_client()
    log.info("Running Databricks Job now", job_id=job_id, notebook_params=notebook_params, python_params=python_params)

    # Call run_now with optional override parameters
    run = db.jobs.run_now(
        job_id=job_id,
        notebook_params=notebook_params,
        python_params=python_params,
        jar_params=jar_params,
        spark_submit_params=spark_submit_params,
        # Add other param types here if supported by the tool signature
    ).result() # Use .result() to block and wait for completion

    # Fetch final run details after waiting
    run_details = db.jobs.get_run(run_id=run.run_id)
    final_state = str(run_details.state.life_cycle_state.value) if run_details.state and run_details.state.life_cycle_state else "UNKNOWN"
    result_state = str(run_details.state.result_state.value) if run_details.state and run_details.state.result_state else "UNKNOWN"

    log.info(
        "Job run finished",
        job_id=job_id,
        run_id=run.run_id,
        life_cycle_state=final_state,
        result_state=result_state,
    )

    return {
        "run_id": run.run_id,
        "status": final_state, # e.g., TERMINATED, SKIPPED, INTERNAL_ERROR
        "result_state": result_state, # e.g., SUCCESS, FAILED, TIMEDOUT, CANCELED
        "run_page_url": run_details.run_page_url,
    }
