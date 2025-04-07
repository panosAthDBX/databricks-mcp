import base64

import structlog
from databricks.sdk.service import workspace as workspace_service
from mcp import Parameter
from mcp import Resource
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Resource.from_callable(
    "databricks:workspace:list_items",
    description="Lists items (notebooks, folders, files, repos) within a specified workspace path.",
    parameters=[
        Parameter(
            name="path",
            description="The absolute path in the workspace to list items from (e.g., '/Users/user@example.com/').",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def list_workspace_items(path: str) -> list[dict]:
    """
    Lists items (notebooks, folders, files, repos, libraries) within a specified workspace path.
    REQ-WS-RES-01
    """
    db = get_db_client()
    log.info("Listing workspace items", path=path)
    # The SDK's list operation handles pagination automatically.
    items = db.workspace.list(path=path)
    result = [
        {
            "path": item.path,
            "type": str(item.object_type.value) if item.object_type else "UNKNOWN", # e.g., NOTEBOOK, DIRECTORY, FILE, REPO
            "object_id": item.object_id,
        }
        for item in items if item.path is not None # Filter out potential null paths
    ]
    log.info("Successfully listed workspace items", path=path, count=len(result))
    return result

@map_databricks_errors
@Resource.from_callable(
    "databricks:workspace:get_notebook_content",
    description="Retrieves the content of a specified notebook.",
    parameters=[
        Parameter(
            name="path",
            description="The absolute path of the notebook in the workspace.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def get_notebook_content(path: str) -> dict:
    """
    Retrieves the content of a specified notebook.
    REQ-WS-RES-02
    """
    db = get_db_client()
    log.info("Getting notebook content", path=path)
    # Export notebook content. Default format is SOURCE.
    exported = db.workspace.export(path=path, format=workspace_service.ExportFormat.SOURCE)

    content = exported.content
    # Content is base64 encoded, decode it.
    try:
        if content:
            decoded_content = base64.b64decode(content).decode('utf-8')
        else:
            decoded_content = ""
    except Exception as e:
        log.error("Failed to decode notebook content", path=path, error=str(e))
        decoded_content = "Error: Failed to decode content." # Provide error in content

    # Determine language - this might require parsing the notebook or is sometimes in metadata
    # The export result doesn't directly give language, but WorkspaceObject might?
    # Let's try getting object details first.
    language = "UNKNOWN"
    try:
        obj_details = db.workspace.get_status(path=path)
        if obj_details.language:
            language = str(obj_details.language.value)
    except Exception:
        log.warning("Could not determine notebook language from status", path=path)


    result = {
        "path": path,
        "content": decoded_content,
        "language": language,
    }
    log.info("Successfully retrieved notebook content", path=path, language=language)
    return result

@map_databricks_errors
@Resource.from_callable(
    "databricks:repos:list",
    description="Lists configured Databricks Repos in the workspace.",
    parameters=[]
)
def list_repos() -> list[dict]:
    """
    Lists configured Databricks Repos.
    REQ-WS-RES-03
    """
    db = get_db_client()
    log.info("Listing Databricks Repos")
    repos = db.repos.list() # Handles pagination
    result = [
        {
            "id": repo.id,
            "path": repo.path,
            "url": repo.url,
            "branch": repo.branch,
            "head_commit_id": repo.head_commit_id,
        }
        for repo in repos if repo.id is not None
    ]
    log.info("Successfully listed Repos", count=len(result))
    return result

@map_databricks_errors
@Resource.from_callable(
    "databricks:repos:get_status",
    description="Gets the status (branch, commit) of a specified Databricks Repo.",
     parameters=[
        Parameter(
            name="repo_id",
            description="The unique identifier of the Databricks Repo.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def get_repo_status(repo_id: str) -> dict:
    """
    Gets the Git status of a specified Databricks Repo.
    REQ-WS-RES-04
    """
    db = get_db_client()
    log.info("Getting status for Databricks Repo", repo_id=repo_id)
    repo_info = db.repos.get(repo_id=repo_id)
    result = {
        "repo_id": repo_info.id,
        "url": repo_info.url,
        "branch": repo_info.branch,
        "head_commit_id": repo_info.head_commit_id,
        # Add other fields like sparse_checkout, path if needed
    }
    log.info("Successfully retrieved Repo status", repo_id=repo_id, branch=result["branch"])
    return result
