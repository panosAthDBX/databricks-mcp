import structlog
import time
import json
# Import the mcp instance from app.py
from ..app import mcp
from databricks.sdk.service import sql as sql_service

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.tool(
    name="databricks:sql:execute_statement",
    description=(
        "Submits a SQL query for asynchronous execution against a specified SQL Warehouse. "
        "Returns a statement_id to check status and retrieve results later using 'get_statement_result'."
    ),
)
def execute_sql(sql_query: str, warehouse_id: str, catalog: str | None = None, schema: str | None = None) -> dict:
    """
    Executes a SQL query asynchronously against a specified SQL Warehouse.
    REQ-DATA-TOOL-01

    Args:
        sql_query: The SQL query text to execute.
        warehouse_id: The ID of the SQL Warehouse to run the query on.
        catalog: Optional catalog context for the query.
        schema: Optional schema context for the query.
    """
    db = get_db_client()
    log.info(
        "Submitting SQL query",
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        query=sql_query[:100] + "..." if len(sql_query) > 100 else sql_query # Log truncated query
    )

    # Use disposition='EXTERNAL_LINKS' for potentially large results, or 'INLINE' otherwise.
    # EXTERNAL_LINKS is generally safer. Let's default to that.
    # Set wait_timeout to 0s for fully async execution.
    resp = db.statement_execution.execute_statement(
        statement=sql_query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="0s", # Ensures the call returns immediately
        disposition=sql_service.Disposition.EXTERNAL_LINKS,
    )

    statement_id = resp.statement_id
    status = str(resp.status.state.value) if resp.status and resp.status.state else "UNKNOWN"

    log.info("SQL query submitted", statement_id=statement_id, initial_status=status)
    return {"statement_id": statement_id, "status": status}


@map_databricks_errors
@mcp.tool(
    name="databricks:sql:get_statement_result",
    description=(
        "Retrieves the status and results (if available) for a previously submitted SQL statement. "
        "Use this to check on queries submitted via 'execute_statement'."
    ),
)
def get_statement_result(statement_id: str) -> dict:
    """
    Retrieves results for a previously executed SQL statement.
    REQ-DATA-TOOL-02

    Args:
        statement_id: The ID of the SQL statement previously submitted.
    """
    db = get_db_client()
    log.info("Getting SQL statement status/result", statement_id=statement_id)

    statement = db.statement_execution.get_statement(statement_id=statement_id)
    status = str(statement.status.state.value) if statement.status and statement.status.state else "UNKNOWN"

    result_data = None
    result_schema = None
    error_message = None

    if status == sql_service.StatementState.SUCCEEDED.value:
        log.debug("Statement succeeded, fetching result chunk", statement_id=statement_id)
        # Fetch the first chunk of results. Pagination might be needed for large results.
        # The SDK might handle this internally, or we might need chunk_index param.
        # For now, fetch the first chunk (index 0).
        chunk = db.statement_execution.get_statement_result_chunk_n(statement_id=statement_id, chunk_index=0)
        if chunk.result and chunk.result.data_array:
            if chunk.result.manifest and chunk.result.manifest.schema and chunk.result.manifest.schema.columns:
                columns = [col.name for col in chunk.result.manifest.schema.columns]
                result_schema = [col.as_dict() for col in chunk.result.manifest.schema.columns] # Provide schema info
                result_data = [dict(zip(columns, row)) for row in chunk.result.data_array]
            else:
                result_data = chunk.result.data_array # Return raw array if no schema
                log.warning("Could not get column names for statement result", statement_id=statement_id)

    elif status == sql_service.StatementState.FAILED.value:
         error_message = statement.status.error.message if statement.status and statement.status.error else "Unknown error"
         log.warning("Statement failed", statement_id=statement_id, error=error_message)

    log.info("Retrieved statement status/result", statement_id=statement_id, status=status)
    return {
        "statement_id": statement_id,
        "status": status,
        "schema": result_schema, # List of column dicts or None
        "result_data": result_data, # List of dicts, raw list, or None
        "error_message": error_message # Error message if failed, else None
    }


@map_databricks_errors
@mcp.tool(
    name="databricks:sql:start_warehouse",
    description="Starts a stopped Databricks SQL Warehouse.",
)
def start_sql_warehouse(warehouse_id: str) -> dict:
    """
    Starts a stopped SQL Warehouse.
    REQ-DATA-TOOL-03
    This is synchronous and waits for the warehouse to start.

    Args:
        warehouse_id: The unique identifier of the SQL Warehouse to start.
    """
    db = get_db_client()
    log.info("Starting SQL Warehouse", warehouse_id=warehouse_id)
    db.warehouses.start(id=warehouse_id).result() # Use .result() to wait
    status = "STARTED" # Assume success if no exception
    log.info("Successfully started SQL Warehouse", warehouse_id=warehouse_id, status=status)
    return {"warehouse_id": warehouse_id, "status": status}


@map_databricks_errors
@mcp.tool(
    name="databricks:sql:stop_warehouse",
    description="Stops a running Databricks SQL Warehouse.",
)
def stop_sql_warehouse(warehouse_id: str) -> dict:
    """
    Stops a running SQL Warehouse.
    REQ-DATA-TOOL-04
    This is synchronous and waits for the warehouse to stop.

    Args:
        warehouse_id: The unique identifier of the SQL Warehouse to stop.
    """
    db = get_db_client()
    log.info("Stopping SQL Warehouse", warehouse_id=warehouse_id)
    db.warehouses.stop(id=warehouse_id).result() # Use .result() to wait
    status = "STOPPED" # Assume success if no exception
    log.info("Successfully stopped SQL Warehouse", warehouse_id=warehouse_id, status=status)
    return {"warehouse_id": warehouse_id, "status": status}
