from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import catalog as uc
from databricks.sdk.service import sql as sql_service
from databricks_mcp import error_mapping as mcp_errors # For preview error test

from databricks_mcp.resources.data import get_table_schema
from databricks_mcp.resources.data import list_catalogs
from databricks_mcp.resources.data import list_schemas
from databricks_mcp.resources.data import list_sql_warehouses
from databricks_mcp.resources.data import list_tables
from databricks_mcp.resources.data import preview_table


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_data():
    mock_client = MagicMock()
    with patch('databricks_mcp.resources.data.get_db_client', return_value=mock_client) as mock_get:
        yield mock_client

# --- Tests for list_catalogs ---
def test_list_catalogs_success(mock_db_client_data):
    cat1 = MagicMock()
    cat1.name = "main"
    cat1.comment = "Main catalog"
    cat1.owner = "admin"
    mock_db_client_data.catalogs.list.return_value = [cat1]
    result = list_catalogs()
    assert len(result) == 1
    assert result[0] == {"name": "main", "comment": "Main catalog", "owner": "admin"}
    mock_db_client_data.catalogs.list.assert_called_once()

# --- Tests for list_schemas ---
def test_list_schemas_success(mock_db_client_data):
    sch1 = MagicMock()
    sch1.name = "default"
    sch1.catalog_name = "main"
    sch1.comment = "Default schema"
    sch1.owner = "admin"
    mock_db_client_data.schemas.list.return_value = [sch1]
    result = list_schemas(catalog_name="main")
    assert len(result) == 1
    assert result[0] == {
        "name": "default",
        "catalog_name": "main",
        "comment": "Default schema",
        "owner": "admin"
    }
    mock_db_client_data.schemas.list.assert_called_once_with(catalog_name="main")

# --- Tests for list_tables ---
def test_list_tables_success(mock_db_client_data):
    tbl1 = MagicMock()
    tbl1.name = "my_table"
    tbl1.catalog_name = "main"
    tbl1.schema_name = "default"
    tbl1.table_type = uc.TableType.MANAGED
    tbl1.comment = "A test table"
    tbl1.owner = "admin"
    mock_db_client_data.tables.list.return_value = [tbl1]
    result = list_tables(catalog_name="main", schema_name="default")
    assert len(result) == 1
    assert result[0] == {
        "name": "my_table",
        "catalog_name": "main",
        "schema_name": "default",
        "type": "MANAGED",
        "comment": "A test table",
        "owner": "admin"
    }
    mock_db_client_data.tables.list.assert_called_once_with(catalog_name="main", schema_name="default")

# --- Tests for get_table_schema ---
def test_get_table_schema_success(mock_db_client_data):
    col1 = MagicMock()
    col1.name = "id"; col1.type_text = "int"; col1.position = 0; col1.nullable = False; col1.comment = "ID"
    col2 = MagicMock()
    col2.name = "data"; col2.type_text = "string"; col2.position = 1; col2.nullable = True; col2.comment = None

    tbl_info = MagicMock()
    tbl_info.table_type = uc.TableType.EXTERNAL
    tbl_info.columns = [col1, col2]
    tbl_info.comment = "Table comment"
    tbl_info.owner = "admin"

    mock_db_client_data.tables.get.return_value = tbl_info
    result = get_table_schema(catalog_name="cat", schema_name="sch", table_name="tbl")

    assert result["full_name"] == "cat.sch.tbl"
    assert result["type"] == "EXTERNAL"
    assert len(result["columns"]) == 2
    assert result["columns"][0] == {"name": "id", "type": "int", "position": 0, "nullable": False, "comment": "ID"}
    assert result["columns"][1] == {"name": "data", "type": "string", "position": 1, "nullable": True, "comment": None}
    assert result["comment"] == "Table comment"
    mock_db_client_data.tables.get.assert_called_once_with(full_name="cat.sch.tbl")

# --- Tests for preview_table ---
def test_preview_table_success(mock_db_client_data):
    # Arrange
    # 1. Mock finding a running warehouse
    wh_info = MagicMock()
    wh_info.id = "wh123"
    wh_info.state = sql_service.State.RUNNING
    mock_db_client_data.warehouses.list.return_value = [wh_info]

    # 2. Mock statement execution result
    col1_schema = MagicMock()
    col1_schema.name = "col_a"
    col2_schema = MagicMock()
    col2_schema.name = "col_b"
    manifest = MagicMock()
    manifest.schema = MagicMock()
    manifest.schema.columns = [col1_schema, col2_schema]

    result_data_mock = MagicMock()
    result_data_mock.data_array = [[1, "a"], [2, "b"]]
    result_data_mock.manifest = manifest

    status = MagicMock()
    status.state = sql_service.StatementState.SUCCEEDED

    statement_resp = MagicMock()
    statement_resp.status = status
    statement_resp.result = result_data_mock

    # Simulate execute_statement returning a waiter that returns the response
    waiter = MagicMock()
    waiter.result.return_value = statement_resp
    mock_db_client_data.statement_execution.execute_statement.return_value = waiter

    # Act
    preview_result = preview_table(catalog_name="cat", schema_name="sch", table_name="tbl", row_limit=50)

    # Assert
    mock_db_client_data.warehouses.list.assert_called_once()
    mock_db_client_data.statement_execution.execute_statement.assert_called_once_with(
        statement="SELECT * FROM `cat`.`sch`.`tbl` LIMIT 50",
        warehouse_id="wh123",
        wait_timeout='50s'
    )
    waiter.result.assert_called_once()
    assert len(preview_result) == 2
    assert preview_result[0] == {"col_a": 1, "col_b": "a"}
    assert preview_result[1] == {"col_a": 2, "col_b": "b"}

def test_preview_table_no_warehouse(mock_db_client_data):
    # Arrange: No running warehouses found
    mock_db_client_data.warehouses.list.return_value = []
    # Act & Assert
    with pytest.raises(Exception) as exc_info:
        preview_table(catalog_name="cat", schema_name="sch", table_name="tbl")
    # Assert on the wrapped exception from the decorator
    assert "Could not find a running SQL Warehouse for preview" in str(exc_info.value)
    assert "RuntimeError" in str(exc_info.value)
    assert "[MCP Error Code -32603]" in str(exc_info.value) # Should map to Internal Error
    mock_db_client_data.statement_execution.execute_statement.assert_not_called()

def test_preview_table_query_fails(mock_db_client_data):
     # Arrange
    # 1. Mock finding a running warehouse
    wh_info = MagicMock()
    wh_info.id = "wh123"
    wh_info.state = sql_service.State.RUNNING
    mock_db_client_data.warehouses.list.return_value = [wh_info]
    # 2. Mock failed statement execution
    status = MagicMock()
    status.state = sql_service.StatementState.FAILED
    status.error = MagicMock()
    status.error.message = "Syntax error"
    statement_resp = MagicMock()
    statement_resp.status = status
    statement_resp.result = None
    waiter = MagicMock(); waiter.result.return_value = statement_resp
    mock_db_client_data.statement_execution.execute_statement.return_value = waiter
    # Act & Assert
    # Import relevant error codes
    from databricks_mcp.error_mapping import CODE_SERVER_ERROR
    with pytest.raises(Exception) as exc_info:
        preview_table(catalog_name="cat", schema_name="sch", table_name="tbl")
    # Check the wrapped exception message for a statement execution failure
    # The original error is mocked as waiter.result() returning status FAILED
    # This should be caught and likely mapped to CODE_SERVER_ERROR
    assert f"[MCP Error Code {CODE_SERVER_ERROR}]" in str(exc_info.value)
    # The original error message from the mock was "Syntax error"
    assert "Syntax error" in str(exc_info.value)

# --- Tests for list_sql_warehouses ---
def test_list_sql_warehouses_success(mock_db_client_data):
    wh1 = MagicMock()
    wh1.id = "wh1"; wh1.name = "WH One"
    wh1.state = sql_service.State.RUNNING
    wh1.cluster_size = "Medium"; wh1.num_clusters = 1; wh1.creator_name = "u1"
    wh2 = MagicMock()
    wh2.id = "wh2"; wh2.name = "WH Two"
    wh2.state = sql_service.State.STOPPED
    wh2.cluster_size = "Small"; wh2.num_clusters = 0; wh2.creator_name = "u2"

    mock_db_client_data.warehouses.list.return_value = [wh1, wh2]
    result = list_sql_warehouses()

    assert len(result) == 2
    assert result[0] == {"id": "wh1", "name": "WH One", "state": "RUNNING", "cluster_size": "Medium", "num_clusters": 1, "creator_name": "u1"}
    assert result[1]["state"] == "STOPPED"
    mock_db_client_data.warehouses.list.assert_called_once()
