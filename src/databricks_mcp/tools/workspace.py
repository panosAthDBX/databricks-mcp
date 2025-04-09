import structlog
# Import the mcp instance from app.py
from ..app import mcp
# from mcp import Tool, forms # Removed unused imports

# Import compute service for Command Execution types
from databricks.sdk.service import compute

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

# Define language choices for execute_code
from typing import Literal
LanguageOptions = Literal["python", "sql", "scala", "r"]

@map_databricks_errors
# Use the mcp instance decorator
@mcp.tool(
    name="databricks:workspace:run_notebook",
    description=(
        "Runs a Databricks notebook and waits for its completion. "
        "NOTE: This tool currently blocks until the notebook run finishes, fails, or times out."
    ),
)
def run_notebook(notebook_path: str, cluster_id: str | None = None, parameters: dict | None = None) -> dict:
    """
    Executes a Databricks notebook and waits for completion.
    REQ-WS-TOOL-01

    Args:
        notebook_path: The absolute path of the notebook to run.
        cluster_id: Optional ID of the cluster to run on. Defaults might apply.
        parameters: Optional dictionary of parameters for the notebook.
    """
    db = get_db_client()
    log.info("Running Databricks notebook", notebook_path=notebook_path, cluster_id=cluster_id, params=parameters)

    # Use Jobs API run_now for a one-time notebook run.
    task = {
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": parameters or {},
        }
    }
    cluster_spec = {}
    if cluster_id:
         cluster_spec["existing_cluster_id"] = cluster_id

    run = db.jobs.run_now(
        run_name=f"MCP Run: {notebook_path}", # Give it a descriptive name
        tasks=[task],
        **cluster_spec # Unpack cluster spec: existing_cluster_id or new_cluster
    ).result() # Use .result() to block and wait for completion

    run_details = db.jobs.get_run(run_id=run.run_id)
    final_state = str(run_details.state.life_cycle_state.value) if run_details.state and run_details.state.life_cycle_state else "UNKNOWN"
    result_state = str(run_details.state.result_state.value) if run_details.state and run_details.state.result_state else "UNKNOWN"

    log.info(
        "Notebook run finished",
        notebook_path=notebook_path,
        run_id=run.run_id,
        life_cycle_state=final_state,
        result_state=result_state,
    )

    return {
        "run_id": run.run_id,
        "status": final_state,
        "result_state": result_state,
        "run_page_url": run_details.run_page_url,
    }


@map_databricks_errors
# Use the mcp instance decorator
@mcp.tool(
    name="databricks:workspace:execute_code",
     description=(
        "Executes a snippet of code (Python, SQL, Scala, R) on a specified cluster and waits for completion. "
        "NOTE: This tool currently blocks until the command finishes, fails, or times out."
    ),
)
def execute_code(code: str, language: LanguageOptions, cluster_id: str) -> dict:
    """
    Executes a snippet of code within a specified cluster context and waits for completion.
    REQ-WS-TOOL-02

    Args:
        code: The code snippet to execute.
        language: The language of the code snippet (python, sql, scala, r).
        cluster_id: The ID of the cluster to execute the code on.
    """
    db = get_db_client()
    log.info("Executing code snippet", language=language, cluster_id=cluster_id)

    # Use the Clusters API execute method
    # cmd = db.command_execution.execute( # OLD
    cmd = db.clusters.execute(
        language=language,
        cluster_id=cluster_id,
        command=code
    ).result() # Use .result() to block and wait for completion

    # cmd_status = str(cmd.status) # OLD: Assumes cmd has status directly
    cmd_status = str(cmd.status.value) if cmd.status else "UNKNOWN" # Use .value for Enum
    result_data = None
    result_type = "UNKNOWN"
    if cmd.results:
        log.debug("Processing command results", command_id=cmd.id, has_results_obj=True)
        result_type = str(cmd.results.result_type.value) if cmd.results and cmd.results.result_type else "UNKNOWN"
        log.debug("Determined result type", command_id=cmd.id, type=result_type)
        # Assign data first
        result_data = cmd.results.data if hasattr(cmd.results, 'data') else None
        log.debug("Initial result data assignment", command_id=cmd.id, data=result_data)
        # Overwrite with cause if it's an error (case-insensitive compare)
        if result_type.upper() == compute.ResultType.ERROR.value.upper() and hasattr(cmd.results, 'cause'):
             result_data = cmd.results.cause
             log.debug("Overwrote result data with error cause", command_id=cmd.id, cause=result_data)

    log.info(
        "Code execution finished",
        language=language,
        cluster_id=cluster_id,
        command_id=cmd.id,
        status=cmd_status,
        result_type=result_type,
    )

    return {
        "command_id": cmd.id,
        "status": cmd_status,
        "result_type": result_type,
        "result_data": result_data,
    }
