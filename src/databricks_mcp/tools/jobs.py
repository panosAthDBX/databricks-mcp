import structlog
from mcp import Parameter
from mcp import Tool
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Tool.from_callable(
    "databricks:jobs:run_now",
    description=(
        "Triggers a specific Databricks Job to run immediately and waits for its completion. "
        "Optional parameters can be provided to override job settings for this run. "
        "NOTE: This tool currently blocks until the job run finishes, fails, or times out."
    ),
    parameters=[
        Parameter(
            name="job_id",
            description="The unique identifier of the job to run.",
            param_type=parameters.IntegerType,
            required=True,
        ),
        Parameter(
            name="notebook_params",
            description="Optional dictionary of notebook parameters to override for this run.",
            param_type=parameters.ObjectType(properties={}),
            required=False,
        ),
        Parameter(
            name="python_params",
            description="Optional list of string arguments to pass to a Python script task for this run.",
            param_type=parameters.ArrayType(items=parameters.StringType),
            required=False,
        ),
        Parameter(
            name="jar_params",
            description="Optional list of string arguments to pass to a JAR task for this run.",
            param_type=parameters.ArrayType(items=parameters.StringType),
            required=False,
        ),
        Parameter(
            name="spark_submit_params",
            description="Optional list of string arguments to pass to a Spark submit task for this run.",
            param_type=parameters.ArrayType(items=parameters.StringType),
            required=False,
        ),
        # Add other override parameters if needed (e.g., python_named_params, sql_params)
    ]
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
