import structlog
from mcp import Parameter
from mcp import Tool
from mcp import forms
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

# Define language choices for execute_code
LANGUAGE_CHOICES = forms.Choices(
    [
        forms.Choice(title="Python", value="python"),
        forms.Choice(title="SQL", value="sql"),
        forms.Choice(title="Scala", value="scala"),
        forms.Choice(title="R", value="r"),
    ]
)

@map_databricks_errors
@Tool.from_callable(
    "databricks:workspace:run_notebook",
    description=(
        "Runs a Databricks notebook and waits for its completion. "
        "NOTE: This tool currently blocks until the notebook run finishes, fails, or times out."
    ),
    parameters=[
        Parameter(
            name="notebook_path",
            description="The absolute path of the notebook to run.",
            param_type=parameters.StringType,
            required=True,
        ),
        Parameter(
            name="cluster_id",
            description="The ID of the cluster to run the notebook on. If not provided, attempts to use a suitable default cluster.",
            param_type=parameters.StringType,
            required=False, # Making optional as job might define cluster or could use default
        ),
         Parameter(
            name="parameters",
            description="A dictionary of parameters to pass to the notebook.",
            param_type=parameters.ObjectType(properties={}), # Generic object/dict
            required=False,
        ),
    ]
)
def run_notebook(notebook_path: str, cluster_id: str | None = None, parameters: dict | None = None) -> dict:
    """
    Executes a Databricks notebook and waits for completion.
    REQ-WS-TOOL-01
    """
    db = get_db_client()
    log.info("Running Databricks notebook", notebook_path=notebook_path, cluster_id=cluster_id, params=parameters)

    # Use Jobs API run_now for a one-time notebook run.
    # This requires defining a task structure.
    task = {
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": parameters or {},
        }
    }
    # Define cluster specification - use existing cluster if provided
    cluster_spec = {}
    if cluster_id:
         cluster_spec["existing_cluster_id"] = cluster_id
    # Add logic here if we need to define a *new* cluster if cluster_id is None
    # else:
    #    log.warning("No cluster_id provided for run_notebook, job may fail if task doesn't define cluster.")
    #    # Or define a default new cluster if desired:
    #    # cluster_spec["new_cluster"] = { ... default cluster spec ... }


    run = db.jobs.run_now(
        run_name=f"MCP Run: {notebook_path}", # Give it a descriptive name
        tasks=[task],
        **cluster_spec # Unpack cluster spec: existing_cluster_id or new_cluster
        # Timeout can be added here if needed: timeout_seconds=...
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

    # Consider what output is most useful. Run page URL? State? Output?
    # Getting actual notebook output might require more API calls if run via jobs.
    return {
        "run_id": run.run_id,
        "status": final_state, # e.g., TERMINATED, SKIPPED, INTERNAL_ERROR
        "result_state": result_state, # e.g., SUCCESS, FAILED, TIMEDOUT, CANCELED
        "run_page_url": run_details.run_page_url,
    }


@map_databricks_errors
@Tool.from_callable(
    "databricks:workspace:execute_code",
     description=(
        "Executes a snippet of code (Python, SQL, Scala, R) on a specified cluster and waits for completion. "
        "NOTE: This tool currently blocks until the command finishes, fails, or times out."
    ),
    parameters=[
         Parameter(
            name="code",
            description="The code snippet to execute.",
            param_type=parameters.StringType,
            required=True,
        ),
        Parameter(
            name="language",
            description="The language of the code snippet.",
            param_type=parameters.StringType,
            form=LANGUAGE_CHOICES, # Use defined choices
            required=True,
        ),
        Parameter(
            name="cluster_id",
            description="The ID of the cluster to execute the code on.",
            param_type=parameters.StringType,
            required=True, # Requires a running cluster
        ),
    ]
)
def execute_code(code: str, language: str, cluster_id: str) -> dict:
    """
    Executes a snippet of code within a specified cluster context and waits for completion.
    REQ-WS-TOOL-02
    """
    db = get_db_client()
    log.info("Executing code snippet", language=language, cluster_id=cluster_id)

    # Use the Command Execution API
    cmd = db.command_execution.execute(
        language=language,
        cluster_id=cluster_id,
        command=code
        # Can add timeout here: command_execution.execute(..., timeout=...)
    ).result() # Use .result() to block and wait for completion

    cmd_status = str(cmd.status) # e.g., CommandStatus.FINISHED, CommandStatus.ERROR
    result_data = None
    result_type = "UNKNOWN"
    if cmd.results:
        result_type = str(cmd.results.result_type) # e.g., ResultType.TEXT, ResultType.ERROR
        result_data = cmd.results.data if hasattr(cmd.results, 'data') else None
        if result_type == "ERROR" and hasattr(cmd.results, 'cause'):
             result_data = cmd.results.cause # Provide error cause if available


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
        "result_data": result_data, # Can be text output, error message, etc.
    }
