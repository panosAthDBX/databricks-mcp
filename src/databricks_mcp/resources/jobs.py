import structlog
# Import the mcp instance from app.py
from ..app import mcp
from databricks.sdk.service import jobs as jobs_service

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.resource(
    "databricks:jobs:list_jobs/{name_filter}/{limit}",
    description="Lists configured Databricks Jobs.",
)
def list_jobs(name_filter: str | None = None, limit: int = 20) -> list[dict]:
    """
    Lists configured Databricks Jobs.
    REQ-JOB-RES-01

    Args:
        name_filter: Optional filter to apply to job names.
        limit: Maximum number of jobs to return (default 20).
    """
    db = get_db_client()
    log.info("Listing Databricks Jobs", name_filter=name_filter, limit=limit)
    # The SDK list operation handles pagination implicitly to some extent,
    # but we respect the limit parameter passed.
    jobs = db.jobs.list(name=name_filter, limit=limit)
    result = [
        {
            "job_id": job.job_id,
            "name": job.settings.name if job.settings else None,
            "creator_user_name": job.creator_user_name,
            # Basic schedule info if available
            "schedule_quartz_expr": job.settings.schedule.quartz_cron_expression if job.settings and job.settings.schedule else None,
            "schedule_timezone": job.settings.schedule.timezone_id if job.settings and job.settings.schedule else None,
            "created_time": job.created_time, # Timestamp in milliseconds
        }
        for job in jobs if job.job_id is not None
    ]
    log.info("Successfully listed jobs", count=len(result))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:jobs:get_job_details/{job_id}",
    description="Gets the detailed configuration of a specific Databricks Job.",
)
def get_job_details(job_id: int) -> dict:
    """
    Gets the configuration of a specific job.
    REQ-JOB-RES-02

    Args:
        job_id: The unique identifier of the job.
    """
    db = get_db_client()
    log.info("Getting details for Databricks Job", job_id=job_id)
    job = db.jobs.get(job_id=job_id)

    # Convert job settings (which uses specific task types) to a dict
    # This might require careful handling depending on the level of detail needed.
    # Using as_dict() is a good starting point.
    settings_dict = job.settings.as_dict() if job.settings else {}

    result = {
        "job_id": job.job_id,
        "creator_user_name": job.creator_user_name,
        "created_time": job.created_time,
        "run_as_user_name": job.run_as_user_name,
        "settings": settings_dict, # Include the full settings structure
    }
    log.info("Successfully retrieved job details", job_id=job_id)
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:jobs:list_job_runs/{job_id}/{limit}/{status_filter}",
    description="Lists recent runs for a specific Databricks job.",
)
def list_job_runs(job_id: int, limit: int = 25, status_filter: str | None = None) -> list[dict]:
    """
    Lists recent runs for a specific job.
    REQ-JOB-RES-03

    Args:
        job_id: The unique identifier of the job.
        limit: Maximum number of job runs to return (default 25).
        status_filter: Optional: Filter runs by life cycle state (e.g., 'PENDING', 'RUNNING', 'TERMINATED').
    """
    db = get_db_client()
    log.info("Listing job runs", job_id=job_id, limit=limit, status_filter=status_filter)

    runs = db.jobs.list_runs(
        job_id=job_id,
        limit=limit,
        # The SDK might expect enum types for filters if available, check SDK docs
        # life_cycle_state=status_filter if status_filter else None # Simplified for now
    )
    result = [
        {
            "run_id": run.run_id,
            "job_id": run.job_id,
            "start_time": run.start_time, # Timestamp ms
            "end_time": run.end_time, # Timestamp ms
            "duration": run.execution_duration, # ms
            "state_life_cycle": str(run.state.life_cycle_state.value) if run.state and run.state.life_cycle_state else "UNKNOWN",
            "state_result": str(run.state.result_state.value) if run.state and run.state.result_state else "UNKNOWN",
            "state_message": run.state.state_message if run.state else None,
            "run_page_url": run.run_page_url,
            "trigger_type": str(run.trigger.value) if run.trigger else None, # e.g., PERIODIC, ONE_TIME
        }
         # Add filtering based on status_filter here if SDK doesn't handle it directly
        for run in runs if run.run_id is not None and (not status_filter or (run.state and run.state.life_cycle_state and run.state.life_cycle_state.value == status_filter.upper()))
    ]
    log.info("Successfully listed job runs", job_id=job_id, count=len(result))
    return result
