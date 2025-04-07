import pytest
import json
from unittest.mock import MagicMock, patch
from databricks.sdk.service import sql as sql_service

from databricks_mcp.tools.data import (
    execute_sql,
    get_statement_result,
    start_sql_warehouse,
    stop_sql_warehouse
)
from databricks_mcp.db_client import get_db_client # To mock

# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_data_tools():
    mock_client = MagicMock()

    # Mock statement execution responses
    mock_exec_status = MagicMock(spec=sql_service.StatementStatus)
    mock_exec_status.state = sql_service.StatementState.PENDING
    mock_exec_resp = MagicMock(spec=sql_service.ExecuteStatementResponse)
    mock_exec_resp.statement_id = "stmt-123"
    mock_exec_resp.status = mock_exec_status
    mock_client.statement_execution.execute_statement.return_value = mock_exec_resp

    # Mock get statement response (initial)
    mock_get_stmt_initial = MagicMock(spec=sql_service.StatementResponse)
    mock_get_stmt_initial.statement_id = "stmt-123"
    mock_get_stmt_initial.status = mock_exec_status # Still pending initially
    mock_get_stmt_initial.result = None
    # Mock get statement response (success) - setup later in test
    # Mock get statement chunk response (success) - setup later in test

    # Mock get statement response (failed) - setup later in test

    # Mock warehouse start/stop
    mock_start_waiter = MagicMock()
    mock_start_waiter.result.return_value = None
    mock_stop_waiter = MagicMock()
    mock_stop_waiter.result.return_value = None
    mock_client.warehouses.start.return_value = mock_start_waiter
    mock_client.warehouses.stop.return_value = mock_stop_waiter

    # Assign unused variable to _
    with patch('databricks_mcp.tools.data.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for execute_sql ---
def test_execute_sql_success(mock_db_client_data_tools):
    query = "SELECT * FROM my_table"
    wh_id = "wh-abc"
    result = execute_sql(sql_query=query, warehouse_id=wh_id)

    mock_db_client_data_tools.statement_execution.execute_statement.assert_called_once_with(
        statement=query,
        warehouse_id=wh_id,
        catalog=None,
        schema=None,
        wait_timeout="0s",
        disposition=sql_service.Disposition.EXTERNAL_LINKS
    )
    assert result == {"statement_id": "stmt-123", "status": "PENDING"}

# --- Tests for get_statement_result ---
def test_get_statement_result_pending(mock_db_client_data_tools):
     # Arrange: Default mock returns pending state
    mock_db_client_data_tools.statement_execution.get_statement.return_value = \
        mock_db_client_data_tools.statement_execution.execute_statement.return_value

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    mock_db_client_data_tools.statement_execution.get_statement.assert_called_once_with(statement_id="stmt-123")
    assert result == {
        "statement_id": "stmt-123",
        "status": "PENDING",
        "schema": None,
        "result_data": None,
        "error_message": None
    }
    mock_db_client_data_tools.statement_execution.get_statement_result_chunk_n.assert_not_called()


def test_get_statement_result_success(mock_db_client_data_tools):
    # Arrange: Mock successful completion and result chunk
    mock_status_succ = MagicMock(spec=sql_service.StatementStatus)
    mock_status_succ.state = sql_service.StatementState.SUCCEEDED
    mock_get_stmt_succ = MagicMock(spec=sql_service.StatementResponse)
    mock_get_stmt_succ.statement_id = "stmt-123"
    mock_get_stmt_succ.status = mock_status_succ

    col1_schema = MagicMock(spec=sql_service.ColumnInfo)
    col1_schema.name = "id"
    col1_schema.as_dict.return_value = {"name": "id"}
    col2_schema = MagicMock(spec=sql_service.ColumnInfo)
    col2_schema.name = "val"
    col2_schema.as_dict.return_value = {"name": "val"}
    manifest = MagicMock(spec=sql_service.ResultManifest)
    manifest.schema = MagicMock(spec=sql_service.Schema)
    manifest.schema.columns = [col1_schema, col2_schema]

    result_data = MagicMock(spec=sql_service.ResultData)
    result_data.data_array = [[1, "a"], [2, "b"]]
    result_data.manifest = manifest

    mock_chunk = MagicMock(spec=sql_service.FetchStatementResultResponse)
    mock_chunk.result = result_data

    mock_db_client_data_tools.statement_execution.get_statement.return_value = mock_get_stmt_succ
    mock_db_client_data_tools.statement_execution.get_statement_result_chunk_n.return_value = mock_chunk

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    mock_db_client_data_tools.statement_execution.get_statement.assert_called_once_with(statement_id="stmt-123")
    mock_db_client_data_tools.statement_execution.get_statement_result_chunk_n.assert_called_once_with(statement_id="stmt-123", chunk_index=0)
    assert result["status"] == "SUCCEEDED"
    assert result["schema"] == [{"name": "id"}, {"name": "val"}]
    assert result["result_data"] == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    assert result["error_message"] is None

def test_get_statement_result_failed(mock_db_client_data_tools):
    # Arrange: Mock failed state
    mock_status_fail = MagicMock(spec=sql_service.StatementStatus)
    mock_status_fail.state = sql_service.StatementState.FAILED
    mock_status_fail.error = MagicMock(spec=sql_service.Error)
    mock_status_fail.error.message = "SQL error occurred"
    mock_get_stmt_fail = MagicMock(spec=sql_service.StatementResponse)
    mock_get_stmt_fail.statement_id = "stmt-123"
    mock_get_stmt_fail.status = mock_status_fail

    mock_db_client_data_tools.statement_execution.get_statement.return_value = mock_get_stmt_fail

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    mock_db_client_data_tools.statement_execution.get_statement.assert_called_once_with(statement_id="stmt-123")
    assert result["status"] == "FAILED"
    assert result["result_data"] is None
    assert result["schema"] is None
    assert result["error_message"] == "SQL error occurred"
    mock_db_client_data_tools.statement_execution.get_statement_result_chunk_n.assert_not_called()


# --- Tests for start_sql_warehouse ---
def test_start_sql_warehouse_success(mock_db_client_data_tools):
    result = start_sql_warehouse(warehouse_id="wh-start")
    mock_db_client_data_tools.warehouses.start.assert_called_once_with(id="wh-start")
    mock_db_client_data_tools.warehouses.start.return_value.result.assert_called_once()
    assert result == {"warehouse_id": "wh-start", "status": "STARTED"}

# --- Tests for stop_sql_warehouse ---
def test_stop_sql_warehouse_success(mock_db_client_data_tools):
    result = stop_sql_warehouse(warehouse_id="wh-stop")
    mock_db_client_data_tools.warehouses.stop.assert_called_once_with(id="wh-stop")
    mock_db_client_data_tools.warehouses.stop.return_value.result.assert_called_once()
    assert result == {"warehouse_id": "wh-stop", "status": "STOPPED"}

# Add tests for SDK error mapping if needed
