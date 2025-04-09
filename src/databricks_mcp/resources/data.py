import structlog
import json
# Import the mcp instance from app.py
from ..app import mcp
from databricks.sdk.service import catalog as catalog_service, sql as sql_service
# Remove mcp errors import

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors, CODE_SERVER_ERROR # Import error code

log = structlog.get_logger(__name__)

# --- Unity Catalog Resources ---

@map_databricks_errors
@mcp.resource(
    "databricks:uc:list_catalogs",
    description="Lists available Unity Catalogs accessible by the current user.",
)
def list_catalogs() -> list[dict]:
    """
    Lists available Unity Catalogs.
    REQ-DATA-RES-01
    """
    db = get_db_client()
    log.info("Listing Unity Catalogs")
    catalogs = db.catalogs.list()
    result = [
        {
            "name": cat.name,
            "comment": cat.comment,
            "owner": cat.owner,
            # Add other fields like created_at, metastore_id if useful
        }
        for cat in catalogs if cat.name
    ]
    log.info("Successfully listed catalogs", count=len(result))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:uc:list_schemas/{catalog_name}",
    description="Lists schemas (databases) within a specified Unity Catalog.",
)
def list_schemas(catalog_name: str) -> list[dict]:
    """
    Lists schemas within a specified catalog.
    REQ-DATA-RES-02

    Args:
        catalog_name: The name of the catalog.
    """
    db = get_db_client()
    log.info("Listing schemas in catalog", catalog_name=catalog_name)
    schemas = db.schemas.list(catalog_name=catalog_name)
    result = [
        {
            "name": sch.name,
            "catalog_name": sch.catalog_name,
            "comment": sch.comment,
            "owner": sch.owner,
        }
        for sch in schemas if sch.name
    ]
    log.info("Successfully listed schemas", catalog_name=catalog_name, count=len(result))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:uc:list_tables/{catalog_name}/{schema_name}",
    description="Lists tables and views within a specified schema in a Unity Catalog.",
)
def list_tables(catalog_name: str, schema_name: str) -> list[dict]:
    """
    Lists tables/views within a specified schema.
    REQ-DATA-RES-03

    Args:
        catalog_name: The name of the catalog.
        schema_name: The name of the schema (database).
    """
    db = get_db_client()
    log.info("Listing tables in schema", catalog_name=catalog_name, schema_name=schema_name)
    tables = db.tables.list(catalog_name=catalog_name, schema_name=schema_name)
    result = [
        {
            "name": tbl.name,
            "catalog_name": tbl.catalog_name,
            "schema_name": tbl.schema_name,
            "type": str(tbl.table_type.value) if tbl.table_type else "UNKNOWN", # e.g., MANAGED, EXTERNAL, VIEW
            "comment": tbl.comment,
            "owner": tbl.owner,
        }
        for tbl in tables if tbl.name
    ]
    log.info("Successfully listed tables", catalog_name=catalog_name, schema_name=schema_name, count=len(result))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:uc:get_table_schema/{catalog_name}/{schema_name}/{table_name}",
    description="Retrieves the schema (column names and types) for a specified table or view.",
)
def get_table_schema(catalog_name: str, schema_name: str, table_name: str) -> dict:
    """
    Retrieves the schema definition for a specified table.
    REQ-DATA-RES-04

    Args:
        catalog_name: The name of the catalog.
        schema_name: The name of the schema (database).
        table_name: The name of the table or view.
    """
    db = get_db_client()
    full_name = f"{catalog_name}.{schema_name}.{table_name}"
    log.info("Getting table schema", table_full_name=full_name)
    table_info = db.tables.get(full_name=full_name)

    columns = []
    if table_info.columns:
        columns = [
            {
                "name": col.name,
                "type": col.type_text, # Or type_name, type_json depending on desired format
                "position": col.position,
                "nullable": col.nullable,
                "comment": col.comment,
            }
            for col in table_info.columns
        ]

    result = {
        "full_name": full_name,
        "type": str(table_info.table_type.value) if table_info.table_type else "UNKNOWN",
        "columns": columns,
        "comment": table_info.comment,
        "owner": table_info.owner,
    }
    log.info("Successfully retrieved table schema", table_full_name=full_name, column_count=len(columns))
    return result

@map_databricks_errors
@mcp.resource(
    "databricks:uc:preview_table/{catalog_name}/{schema_name}/{table_name}/{row_limit}",
    description="Retrieves the first N rows of a specified table or view.",
)
def preview_table(catalog_name: str, schema_name: str, table_name: str, row_limit: int = 100) -> list[dict]:
    """
    Retrieves the first N rows of a specified table using SQL query.
    REQ-DATA-RES-05

    Note: This requires a running SQL Warehouse and uses the Statement Execution API.
    It might be better implemented as a Tool if warehouse selection is needed,
    but is kept as a Resource here for simplicity, assuming a default/available warehouse.
    Consider adding warehouse_id parameter if needed.

    Args:
        catalog_name: The name of the catalog.
        schema_name: The name of the schema (database).
        table_name: The name of the table or view.
        row_limit: The maximum number of rows to retrieve (default 100).
    """
    db = get_db_client()
    full_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`" # Use backticks for safety
    sql_query = f"SELECT * FROM {full_name} LIMIT {row_limit}"
    log.info("Previewing table", table_full_name=full_name, limit=row_limit, query=sql_query)

    # Find an available warehouse (simple approach: find first running one)
    warehouse_id = None
    try:
        warehouses = db.warehouses.list()
        for w in warehouses:
            if w.state == sql_service.State.RUNNING:
                warehouse_id = w.id
                log.debug("Using warehouse for preview", warehouse_id=warehouse_id)
                break
        if not warehouse_id:
             raise RuntimeError("No running SQL Warehouse found to execute preview query.")
    except Exception as e:
         log.error("Failed to find suitable SQL warehouse", error=e)
         raise RuntimeError("Could not find a running SQL Warehouse for preview.") from e

    statement = db.statement_execution.execute_statement(
        statement=sql_query,
        warehouse_id=warehouse_id,
        wait_timeout='50s' # Short timeout for preview
    ).result() # Block until finished or timeout

    if statement.status.state != sql_service.StatementState.SUCCEEDED:
         err_msg = statement.status.error.message if statement.status.error else 'Unknown error'
         log.error("Preview query failed", table_full_name=full_name, state=statement.status.state, error_msg=err_msg)
         # Raise a standard Exception, decorator will map if possible, otherwise MCP handles generic Exception
         raise Exception(f"[MCP Error Code {CODE_SERVER_ERROR}] Failed to preview table. State: {statement.status.state}. Error: {err_msg}")

    result_data = []
    if statement.result and statement.result.data_array:
        columns = [col.name for col in statement.result.manifest.schema.columns] if statement.result.manifest and statement.result.manifest.schema else []
        if columns:
            result_data = [dict(zip(columns, row)) for row in statement.result.data_array]
        else:
             result_data = statement.result.data_array
             log.warning("Could not get column names for preview, returning data as arrays.", table_full_name=full_name)

    log.info("Successfully previewed table", table_full_name=full_name, rows_retrieved=len(result_data))
    return result_data


# --- SQL Warehouse Resources ---

@map_databricks_errors
@mcp.resource(
    "databricks:sql:list_warehouses",
    description="Lists available Databricks SQL Warehouses.",
)
def list_sql_warehouses() -> list[dict]:
    """
    Lists available SQL Warehouses.
    REQ-DATA-RES-06
    """
    db = get_db_client()
    log.info("Listing SQL Warehouses")
    warehouses = db.warehouses.list()
    result = [
        {
            "id": wh.id,
            "name": wh.name,
            "state": str(wh.state.value) if wh.state else "UNKNOWN", # e.g., RUNNING, STOPPED
            "cluster_size": wh.cluster_size,
            "num_clusters": wh.num_clusters,
            "creator_name": wh.creator_name,
            # Add other fields like channel, jdbc_url etc. if needed
        }
        for wh in warehouses if wh.id
    ]
    log.info("Successfully listed SQL Warehouses", count=len(result))
    return result
