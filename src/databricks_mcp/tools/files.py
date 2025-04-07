import base64

import structlog
from mcp import Parameter
from mcp import Tool
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

# Note: These tools currently primarily use the DBFS API.
# Support for UC Volumes via the Files API might require specific SDK methods
# (e.g., db.files.read, db.files.upload) if available and different from DBFS.
# We assume db.dbfs works for basic cases or add warnings.

@map_databricks_errors
@Tool.from_callable(
    "databricks:files:read",
    description="Reads the content of a file from DBFS or a Volume path.",
    parameters=[
        Parameter(name="path", description="Absolute path of the file to read.", param_type=parameters.StringType, required=True),
        Parameter(name="offset", description="Byte offset to start reading from.", param_type=parameters.IntegerType, required=False, default=0),
        Parameter(name="length", description="Maximum number of bytes to read. Reads whole file if 0 or omitted.", param_type=parameters.IntegerType, required=False, default=0), # DBFS API uses 'length' for max bytes
    ]
)
def read_file(path: str, offset: int = 0, length: int = 0) -> dict:
    """
    Reads the content of a file from DBFS or a Volume.
    REQ-FILE-TOOL-01
    Content is returned base64 encoded.
    """
    db = get_db_client()
    log.info("Reading file", path=path, offset=offset, length=length)

    # DBFS read returns base64 encoded content directly
    # Use length=0 or omit for reading the whole file up to API limit (1MB)
    read_length = length if length > 0 else 1024*1024 # Use API limit if length is 0/omitted
    response = db.dbfs.read(path=path, offset=offset, length=read_length)

    content_b64 = response.data or ""
    bytes_read = response.bytes_read or 0

    # Try to decode for logging preview, but return base64
    preview = "Unable to decode for preview"
    if content_b64:
        try:
            preview_bytes = base64.b64decode(content_b64)
            preview = preview_bytes[:80].decode('utf-8', errors='replace') + ('...' if len(preview_bytes) > 80 else '')
        except Exception:
            pass # Keep default preview message

    log.info("Successfully read file", path=path, bytes_read=bytes_read, preview=preview)
    return {"path": path, "content_base64": content_b64, "bytes_read": bytes_read}


@map_databricks_errors
@Tool.from_callable(
    "databricks:files:write",
    description="Writes content to a file in DBFS or a Volume path. Content should be base64 encoded.",
    parameters=[
        Parameter(name="path", description="Absolute path of the file to write.", param_type=parameters.StringType, required=True),
        Parameter(name="content_base64", description="Base64 encoded content to write.", param_type=parameters.StringType, required=True),
        Parameter(name="overwrite", description="Overwrite the file if it already exists.", param_type=parameters.BooleanType, required=False, default=False),
    ]
)
def write_file(path: str, content_base64: str, overwrite: bool = False) -> dict:
    """
    Writes base64 encoded content to a file in DBFS or a Volume.
    REQ-FILE-TOOL-02
    """
    db = get_db_client()
    bytes_written = len(base64.b64decode(content_base64)) # Calculate original size for logging
    log.info("Writing file", path=path, overwrite=overwrite, input_bytes=bytes_written)

    # DBFS API uses put operation. Requires handle and add_block/close.
    # Let's use the simpler create -> add_block -> close flow.
    # Ensure overwrite behavior is handled correctly.
    try:
        handle = db.dbfs.create(path=path, overwrite=overwrite).handle
        db.dbfs.add_block(handle=handle, data=content_base64)
        db.dbfs.close(handle=handle)
        status = "SUCCESS"
        log.info("Successfully wrote file", path=path, bytes_written=bytes_written)
    except Exception as e:
        # Catch potential errors during write/close
        log.error("Failed during file write operation", path=path, error=str(e))
        status = "FAILED"
        # Re-raise if the decorator doesn't catch it, or let decorator handle
        raise e

    return {"path": path, "status": status, "bytes_written": bytes_written}


@map_databricks_errors
@Tool.from_callable(
    "databricks:files:delete",
    description="Deletes a file or directory (optionally recursively) from DBFS or a Volume path.",
    parameters=[
        Parameter(name="path", description="Absolute path of the file or directory to delete.", param_type=parameters.StringType, required=True),
        Parameter(name="recursive", description="If true, delete directory and its contents recursively.", param_type=parameters.BooleanType, required=False, default=False),
    ]
)
def delete_file(path: str, recursive: bool = False) -> dict:
    """
    Deletes a file or directory from DBFS or a Volume.
    REQ-FILE-TOOL-03
    """
    db = get_db_client()
    log.info("Deleting file/directory", path=path, recursive=recursive)
    db.dbfs.delete(path=path, recursive=recursive)
    status = "SUCCESS"
    log.info("Successfully deleted file/directory", path=path)
    return {"path": path, "status": status}


@map_databricks_errors
@Tool.from_callable(
    "databricks:files:create_directory",
    description="Creates a directory (including any necessary parent directories) in DBFS or a Volume path.",
     parameters=[
        Parameter(name="path", description="Absolute path of the directory to create.", param_type=parameters.StringType, required=True),
    ]
)
def create_directory(path: str) -> dict:
    """
    Creates a directory in DBFS or a Volume.
    REQ-FILE-TOOL-04
    """
    db = get_db_client()
    log.info("Creating directory", path=path)
    db.dbfs.mkdirs(path=path) # mkdirs creates parent directories if needed
    status = "SUCCESS"
    log.info("Successfully created directory", path=path)
    return {"path": path, "status": status}
