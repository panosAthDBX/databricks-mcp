import structlog
# Import the mcp instance from app.py
from ..app import mcp
from databricks.sdk.errors import DatabricksError

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

# --- MLflow Experiment Resources ---

@map_databricks_errors
@mcp.resource(
    "databricks:mlflow:list_experiments/{max_results}",
    description="Lists MLflow experiments.",
)
def list_mlflow_experiments(max_results: int = 100) -> list[dict]:
    """
    Lists MLflow experiments.
    REQ-ML-RES-01

    Args:
        max_results: Maximum number of experiments to return (default 100).
    """
    db = get_db_client()
    log.info("Listing MLflow experiments", limit=max_results)
    experiments = db.experiments.list_experiments(max_results=max_results)
    result = [
        {
            "experiment_id": exp.experiment_id,
            "name": exp.name,
            "artifact_location": exp.artifact_location,
            "lifecycle_stage": exp.lifecycle_stage,
            "creation_time": exp.creation_time,
            "last_update_time": exp.last_update_time,
        }
        for exp in experiments.experiments # API returns Experiment list inside response object
        if exp.experiment_id is not None
    ]
    log.info("Successfully listed MLflow experiments", count=len(result))
    return result


@map_databricks_errors
@mcp.resource(
    "databricks:mlflow:list_runs/{experiment_id}/{filter_string}/{max_results}",
    description="Lists runs for a given MLflow experiment.",
)
def list_mlflow_runs(experiment_id: str, filter_string: str | None = None, max_results: int = 100) -> list[dict]:
    """
    Lists runs for a given MLflow experiment.
    REQ-ML-RES-02

    Args:
        experiment_id: The ID of the experiment.
        filter_string: Optional filter query string (e.g., "metrics.accuracy > 0.9").
        max_results: Maximum number of runs to return (default 100).
    """
    db = get_db_client()
    log.info("Listing MLflow runs", experiment_id=experiment_id, filter=filter_string, limit=max_results)
    # Note: search_runs uses experiment_ids (plural list)
    runs_response = db.experiments.search_runs(
        experiment_ids=[experiment_id],
        filter=filter_string,
        max_results=max_results
    )
    result = [
        {
            "run_id": run.info.run_id,
            "experiment_id": run.info.experiment_id,
            "user_id": run.info.user_id,
            "status": run.info.status, # e.g., RUNNING, FINISHED, FAILED
            "start_time": run.info.start_time,
            "end_time": run.info.end_time,
            "artifact_uri": run.info.artifact_uri,
            "lifecycle_stage": run.info.lifecycle_stage,
        }
        for run in runs_response.runs # API returns Run list inside response object
        if run.info and run.info.run_id is not None
    ]
    log.info("Successfully listed MLflow runs", experiment_id=experiment_id, count=len(result))
    return result


@map_databricks_errors
@mcp.resource(
    "databricks:mlflow:get_run_details/{run_id}",
    description="Gets parameters, metrics, and tags for a specific MLflow run.",
)
def get_mlflow_run_details(run_id: str) -> dict:
    """
    Gets parameters, metrics, and artifacts for a specific MLflow run.
    REQ-ML-RES-03
    Note: Artifacts themselves are not fetched, only the artifact URI.

    Args:
        run_id: The unique identifier of the MLflow run.
    """
    db = get_db_client()
    log.info("Getting MLflow run details", run_id=run_id)
    run = db.experiments.get_run(run_id=run_id).run # API returns Run object inside response

    if not run or not run.info:
        raise DatabricksError(f"Run with ID '{run_id}' not found or info missing.")

    # Extract data, params, metrics, tags
    params = {p.key: p.value for p in run.data.params} if run.data and run.data.params else {}
    metrics = {m.key: m.value for m in run.data.metrics} if run.data and run.data.metrics else {}
    tags = {t.key: t.value for t in run.data.tags} if run.data and run.data.tags else {}


    result = {
        "run_id": run.info.run_id,
        "experiment_id": run.info.experiment_id,
        "user_id": run.info.user_id,
        "status": run.info.status,
        "start_time": run.info.start_time,
        "end_time": run.info.end_time,
        "artifact_uri": run.info.artifact_uri,
        "lifecycle_stage": run.info.lifecycle_stage,
        "params": params,
        "metrics": metrics,
        "tags": tags,
    }
    log.info("Successfully retrieved MLflow run details", run_id=run_id)
    return result

# --- Model Registry Resources ---

@map_databricks_errors
@mcp.resource(
    "databricks:mlflow:list_registered_models/{filter_string}/{max_results}",
    description="Lists models registered in the MLflow Model Registry.",
)
def list_registered_models(filter_string: str | None = None, max_results: int = 100) -> list[dict]:
    """
    Lists models registered in the MLflow Model Registry.
    REQ-ML-RES-04

    Args:
        filter_string: Optional filter query string (e.g., "name like 'my_model%'").
        max_results: Maximum number of models to return (default 100).
    """
    db = get_db_client()
    log.info("Listing registered models", filter=filter_string, limit=max_results)
    models = db.model_registry.search_registered_models(filter=filter_string, max_results=max_results)

    result = [
        {
            "name": model.name,
            "creation_timestamp": model.creation_timestamp,
            "last_updated_timestamp": model.last_updated_timestamp,
            "user_id": model.user_id,
            "description": model.description,
            "latest_versions": [ # Extract key info from latest versions
                 {"name": v.name, "version": v.version, "current_stage": v.current_stage, "status": v.status}
                 for v in model.latest_versions
             ] if model.latest_versions else [],
        }
        for model in models.registered_models # API returns list inside object
        if model.name is not None
    ]
    log.info("Successfully listed registered models", count=len(result))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:mlflow:get_model_version_details/{model_name}/{version}",
    description="Gets details for a specific version of a registered model.",
)
def get_model_version_details(model_name: str, version: str) -> dict:
    """
    Gets details for a specific model version.
    REQ-ML-RES-05

    Args:
        model_name: The name of the registered model.
        version: The version number of the model (e.g., "1").
    """
    db = get_db_client()
    log.info("Getting model version details", model_name=model_name, version=version)
    version_info = db.model_registry.get_model_version(name=model_name, version=version).model_version # API returns object inside response

    if not version_info:
         raise DatabricksError(f"Model version '{version}' for model '{model_name}' not found.")

    result = {
        "name": version_info.name,
        "version": version_info.version,
        "creation_timestamp": version_info.creation_timestamp,
        "last_updated_timestamp": version_info.last_updated_timestamp,
        "user_id": version_info.user_id,
        "current_stage": version_info.current_stage, # e.g., Staging, Production, Archived
        "description": version_info.description,
        "source": version_info.source, # Link to run or artifact path
        "run_id": version_info.run_id,
        "status": version_info.status, # e.g., READY, FAILED_REGISTRATION
        "status_message": version_info.status_message,
        "tags": {t.key: t.value for t in version_info.tags} if version_info.tags else {},
    }
    log.info("Successfully retrieved model version details", model_name=model_name, version=version)
    return result
