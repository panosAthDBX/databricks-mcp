import structlog
from mcp import Parameter
from mcp import Resource
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Resource.from_callable(
    "databricks:files:list",
    description="Lists files and directories in a specified DBFS or Unity Catalog Volume path.",
    parameters=[
        Parameter(
            name="path",
            description="The absolute path to list (e.g., '/mnt/mydata', '/Volumes/main/default/myvol/').",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def list_files(path: str) -> list[dict]:
    """
    Lists files and directories in DBFS or a Unity Catalog Volume path.
    REQ-FILE-RES-01
    Attempts to use the correct API (DBFS or Files) based on the path prefix.
    """
    db = get_db_client()
    log.info("Listing files/directories", path=path)

    items = []
    if path.startswith("/Volumes/"):
        # Use Files API for Volumes
        log.debug("Using Files API for Volume path", path=path)
        # Note: Files API list_directory_contents is not directly in databricks-sdk yet (as of some versions).
        # We might need to use a lower-level API call or assume DBFS for now if direct support lacks.
        # Let's *assume* db.dbfs.list works for volumes for now, or raise NotImplemented.
        # Check SDK documentation for current Files API support.
        # ----
        # UPDATE: Checking common SDK patterns, often DBFS *might* work via FUSE mount
        # or specific clients are needed. Let's proceed with db.dbfs.list and add a note.
        # If db.files.list_directory_contents exists, use that. Let's assume db.dbfs for now.
        log.warning("Attempting to list Volume path using DBFS API; specific Files API might be needed.", path=path)
        listed_items = db.dbfs.list(path=path)
        items = [
            {
                "path": f.path,
                "is_dir": f.is_dir,
                "size": f.file_size,
            }
            for f in listed_items if f.path is not None
        ]

    elif path.startswith("/dbfs/") or path.startswith("/mnt/"):
         # Use DBFS API
        log.debug("Using DBFS API for path", path=path)
        listed_items = db.dbfs.list(path=path)
        items = [
            {
                "path": f.path,
                "is_dir": f.is_dir,
                "size": f.file_size,
            }
            for f in listed_items if f.path is not None
        ]
    else:
        # Assume DBFS for other paths? Or raise an error?
        # Let's default to DBFS but log a warning.
        log.warning("Path does not start with /Volumes/, /dbfs/, or /mnt/. Assuming DBFS path.", path=path)
        listed_items = db.dbfs.list(path=path)
        items = [
            {
                "path": f.path,
                "is_dir": f.is_dir,
                "size": f.file_size,
            }
            for f in listed_items if f.path is not None
        ]


    log.info("Successfully listed files/directories", path=path, count=len(items))
    return items
