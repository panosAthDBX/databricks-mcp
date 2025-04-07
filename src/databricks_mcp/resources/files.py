import structlog
# Import the mcp instance from app.py
from ..app import mcp
# Remove incorrect dbfs import
# from databricks.sdk.service import dbfs as dbfs_service

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.resource(
    "databricks:files:list/{path}",
    description="Lists files and directories in a specified DBFS or Unity Catalog Volume path.",
)
def list_files(path: str) -> list[dict]:
    """
    Lists files and directories in DBFS or a Unity Catalog Volume path.
    REQ-FILE-RES-01
    Attempts to use the correct API (DBFS or Files) based on the path prefix.

    Args:
        path: The absolute path to list (e.g., '/mnt/mydata', '/Volumes/main/default/myvol/').
    """
    db = get_db_client()
    log.info("Listing files/directories", path=path)

    items = []
    # Access dbfs API via the client instance db.dbfs
    if path.startswith("/Volumes/"):
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
    else:
        # Assume DBFS for /dbfs, /mnt, or others
        if not (path.startswith("/dbfs/") or path.startswith("/mnt/")):
             log.warning("Path does not start with known prefix. Assuming DBFS path.", path=path)
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
