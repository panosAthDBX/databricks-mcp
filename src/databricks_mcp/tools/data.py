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
    name="databricks-sql-execute_statement",
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
        disposition=sql_service.Disposition.INLINE, # Try INLINE for direct results
        format=sql_service.Format.JSON_ARRAY, # JSON_ARRAY is most compatible
    )

    statement_id = resp.statement_id
    status = str(resp.status.state.value) if resp.status and resp.status.state else "UNKNOWN"

    log.info("SQL query submitted", statement_id=statement_id, initial_status=status)
    return {"statement_id": statement_id, "status": status}


@map_databricks_errors
@mcp.tool(
    name="databricks-sql-get_statement_result",
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
        log.debug("Statement succeeded, handling results appropriately", statement_id=statement_id)
        
        # Let's first check what we've received
        log.info(
            "Statement response object structure",
            statement_id=statement_id, 
            has_result=hasattr(statement, 'result'),
            has_manifest=hasattr(statement, 'manifest')
        )
        
        try:
            # First check for inline results
            if hasattr(statement, 'result') and statement.result is not None:
                log.info("Statement has result property", statement_id=statement_id)

                # Check for external links (EXTERNAL_LINKS disposition)
                # Be more specific checking the attribute exists and is non-empty list
                external_links = getattr(statement.result, 'external_links', None)
                if external_links is not None and isinstance(external_links, list) and external_links:
                    log.info("External links found - data is stored externally",
                             statement_id=statement_id,
                             link_count=len(external_links))
                    result_data = [
                        {
                            "chunk_index": link.chunk_index,
                            "row_count": link.row_count,
                            "external_link_available": True,
                        }
                        for link in external_links
                    ]

                # Check for inline data array (INLINE disposition)
                # Check attribute exists and is non-empty list
                data_array = getattr(statement.result, 'data_array', None)
                if data_array is not None and isinstance(data_array, list) and data_array:
                    log.info("Inline data_array found",
                             statement_id=statement_id,
                             row_count=len(data_array))
                    result_data = data_array
            
            # Get schema information from manifest
            manifest = getattr(statement, 'manifest', None)
            if manifest:
                log.info("Statement has manifest with schema", statement_id=statement_id)
                
                schema = getattr(manifest, 'schema', None)
                if schema and hasattr(schema, 'columns'):
                    columns = schema.columns
                    result_schema = [
                        {
                            "name": col.name,
                            "type": getattr(col, 'type_text', None),
                            "position": getattr(col, 'position', None)
                        }
                        for col in columns
                    ]
                    
                    # If we have schema and raw data_array, create dict result
                    if result_data and isinstance(result_data, list) and isinstance(result_data[0], list):
                        column_names = []
                        if columns:
                            log.debug("Iterating over schema columns", columns_repr=repr(columns))
                            for i, col in enumerate(columns):
                                log.debug(f"Processing column index {i}", col_repr=repr(col), has_name=hasattr(col, 'name'))
                                # Directly access .name assuming it's set
                                col_name = col.name if hasattr(col, 'name') else None
                                # col_name = getattr(col, 'name', None) # OLD
                                log.debug(f"Extracted col_name for index {i}", name=col_name)
                                if col_name and isinstance(col_name, str): # Ensure it's a string
                                    column_names.append(col_name)
                                else:
                                    log.warning("Column in schema missing name attribute or not a string", column_index=i, column_object=repr(col))
                        else:
                            log.warning("Schema columns attribute was empty or None")

                        log.debug("Final extracted column names", names=column_names)
                        # Only transform if we successfully got column names
                        if column_names and len(column_names) == len(result_data[0]):
                            try:
                                result_data = [dict(zip(column_names, row)) for row in result_data]
                                log.info("Transformed raw data into dictionary format with schema",
                                         statement_id=statement_id,
                                         row_count=len(result_data),
                                         first_row_example=result_data[0] if result_data else None)
                            except Exception as transform_error:
                                log.error("Error during data transformation", error=transform_error, exc_info=True)
                                # Keep raw data if transformation fails
                        else:
                            log.warning("Could not transform data to dict: column name mismatch or missing columns", 
                                        num_columns=len(column_names), 
                                        first_row_len=len(result_data[0]) if result_data else 0)
                            # Keep raw list data if transformation impossible
            
            # If still no result_data after checking inline/external, try fetching chunk
            if result_data is None:
                log.debug("No result data in statement response, trying to fetch chunk", statement_id=statement_id)
                try:
                    chunk = db.statement_execution.get_statement_result_chunk_n(
                        statement_id=statement_id, chunk_index=0
                    )
                    
                    if hasattr(chunk, 'data_array') and chunk.data_array:
                        log.info("Retrieved data from chunk", 
                                 statement_id=statement_id,
                                 row_count=len(chunk.data_array))
                        result_data = chunk.data_array
                        
                        # If we have schema already, apply it to the chunk data
                        if result_schema and isinstance(result_data[0], list):
                            column_names = [col["name"] for col in result_schema]
                            result_data = [dict(zip(column_names, row)) for row in result_data]
                except Exception as e:
                    log.warning(
                        "Failed to fetch chunk data - this is normal for EXTERNAL_LINKS disposition",
                        statement_id=statement_id,
                        error=str(e)
                    )
                
            # If we still don't have result_data, provide a status message
            if result_data is None:
                result_data = [{"message": "Query completed successfully but no data available. Results may be available via external links."}]
                log.info("No data could be directly retrieved - likely external link disposition", statement_id=statement_id)
                
        except Exception as e:
            log.error("Error processing statement results", statement_id=statement_id, error=str(e), exc_info=True)
            # Provide a helpful error message in the result
            result_data = [{"error": f"Error processing results: {str(e)}"}]

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
    name="databricks-sql-start_warehouse",
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
    name="databricks-sql-stop_warehouse",
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
