import base64

import structlog
# Import the mcp instance from app.py
from ..app import mcp
# Import specific service modules needed
from databricks.sdk.service import workspace

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.resource(
    "databricks:workspace:list_items/{path}",
    description="Lists items (notebooks, folders, files, repos) within a specified workspace path.",
)
def list_workspace_items(path: str) -> list[dict]:
    """
    Lists items (notebooks, folders, files, repos, libraries) within a specified workspace path.
    REQ-WS-RES-01

    Args:
        path: The absolute path in the workspace to list items from (e.g., '/Users/user@example.com/').
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
@mcp.resource(
    "databricks:workspace:get_notebook_content/{path}",
    description="Retrieves the content of a specified notebook.",
)
def get_notebook_content(path: str) -> dict:
    """
    Retrieves the content of a specified notebook.
    REQ-WS-RES-02

    Args:
        path: The absolute path of the notebook in the workspace.
    """
    notebook_path = path
    db = get_db_client()
    log.info("Getting notebook content", path=notebook_path)
    # Export notebook content. Default format is SOURCE.
    exported = db.workspace.export(path=notebook_path, format=workspace.ExportFormat.SOURCE)

    content = exported.content
    # Content is base64 encoded, decode it.
    try:
        if content:
            decoded_content = base64.b64decode(content).decode('utf-8')
        else:
            decoded_content = ""
    except Exception as e:
        log.error("Failed to decode notebook content", path=notebook_path, error=str(e))
        decoded_content = "Error: Failed to decode content." # Provide error in content

    # Determine language
    language = "UNKNOWN"
    try:
        obj_details = db.workspace.get_status(path=notebook_path)
        if obj_details.language:
            language = str(obj_details.language.value)
    except Exception:
        log.warning("Could not determine notebook language from status", path=notebook_path)

    result = {
        "path": notebook_path,
        "content": decoded_content,
        "language": language,
    }
    log.info("Successfully retrieved notebook content", path=notebook_path, language=language)
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:repos:list",
    description="Lists configured Databricks Repos in the workspace.",
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
@mcp.resource(
    "databricks:repos:get_status/{repo_id}",
    description="Gets the status (branch, commit) of a specified Databricks Repo.",
)
def get_repo_status(repo_id: str) -> dict:
    """
    Gets the Git status of a specified Databricks Repo.
    REQ-WS-RES-04

    Args:
        repo_id: The unique identifier of the Databricks Repo.
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
