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

    # Mock execute_statement response
    mock_exec_resp = MagicMock()
    mock_exec_resp.statement_id = "stmt-123"
    mock_exec_resp.status = MagicMock()
    # Changed default fixture status to PENDING as execute_statement is async
    mock_exec_resp.status.state = sql_service.StatementState.PENDING
    mock_client.statement_execution.execute_statement.return_value = mock_exec_resp

    # Mock get_statement response
    mock_get_resp = MagicMock()
    mock_get_resp.statement_id = "stmt-123"
    mock_get_resp.status = MagicMock()
    mock_get_resp.status.state = sql_service.StatementState.SUCCEEDED
    mock_get_resp.result = MagicMock()
    mock_get_resp.result.data_array = [[1, "a"], [2, "b"]]
    mock_get_resp.result.manifest = MagicMock()
    # Create a mock schema object explicitly
    mock_schema = MagicMock()
    col1 = MagicMock(name="colA")
    col2 = MagicMock(name="colB")
    mock_schema.columns = [col1, col2] # Assign list to mock schema's columns
    # Assign the mock schema to the manifest
    mock_get_resp.result.manifest.schema = mock_schema
    mock_client.statement_execution.get_statement.return_value = mock_get_resp

    # Mock start/stop warehouse to return a waiter like other blocking calls
    mock_start_waiter = MagicMock()
    mock_start_waiter.result.return_value = None # Simulate successful wait
    mock_stop_waiter = MagicMock()
    mock_stop_waiter.result.return_value = None # Simulate successful wait
    mock_client.warehouses.start.return_value = mock_start_waiter
    mock_client.warehouses.stop.return_value = mock_stop_waiter

    with patch('databricks_mcp.tools.data.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for execute_sql ---
def test_execute_sql_success(mock_db_client_data_tools):
    # Arrange
    query = "SELECT * FROM my_table"
    wh_id = "wh-abc"
    cat = "main"
    sch = "default"

    # Act
    result = execute_sql(sql_query=query, warehouse_id=wh_id, catalog=cat, schema=sch)

    # Assert
    mock_db_client_data_tools.statement_execution.execute_statement.assert_called_once_with(
        statement=query,
        warehouse_id=wh_id,
        catalog=cat,
        schema=sch,
        wait_timeout="0s", # Check actual arguments used in implementation
        disposition=sql_service.Disposition.INLINE, # Check actual arguments
        format=sql_service.Format.JSON_ARRAY, # Check actual arguments
    )
    # Check the initial status from the fixture
    assert result == {"statement_id": "stmt-123", "status": "PENDING"}

# --- Tests for get_statement_result ---
def test_get_statement_result_pending(mock_db_client_data_tools):
    # Arrange - modify fixture return for get_statement to be PENDING
    mock_get_resp = mock_db_client_data_tools.statement_execution.get_statement.return_value
    mock_get_resp.status.state = sql_service.StatementState.PENDING
    mock_get_resp.result = None # Ensure no result data when pending

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    assert result["statement_id"] == "stmt-123"
    assert result["status"] == "PENDING"
    # Assert the key returned by the function (likely 'result_data' or similar)
    assert result["result_data"] is None
    assert result["error_message"] is None

def test_get_statement_result_success(mock_db_client_data_tools):
    # Arrange - OVERRIDE fixture mock for this specific test
    mock_get_resp_success = MagicMock()
    mock_get_resp_success.statement_id = "stmt-123"
    mock_get_resp_success.status = MagicMock()
    mock_get_resp_success.status.state = sql_service.StatementState.SUCCEEDED

    # Setup result with data_array
    mock_get_resp_success.result = MagicMock()
    mock_get_resp_success.result.data_array = [[1, "a"], [2, "b"]]

    # Setup manifest with schema and columns
    mock_get_resp_success.manifest = MagicMock()
    mock_schema = MagicMock()
    # Create mocks THEN set the name attribute
    col1 = MagicMock()
    col1.name = "colA"
    col2 = MagicMock()
    col2.name = "colB"
    mock_schema.columns = [col1, col2]
    mock_get_resp_success.manifest.schema = mock_schema

    # Configure the main mock client's method
    mock_db_client_data_tools.statement_execution.get_statement.return_value = mock_get_resp_success

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    mock_db_client_data_tools.statement_execution.get_statement.assert_called_once_with(statement_id="stmt-123")
    assert result["statement_id"] == "stmt-123"
    assert result["status"] == "SUCCEEDED"
    assert result["error_message"] is None
    assert result["schema"] is not None # Check schema was processed
    assert len(result["schema"]) == 2
    assert result["schema"][0]["name"] == "colA"
    assert result["result_data"] is not None # Check result_data exists
    assert len(result["result_data"]) == 2
    assert result["result_data"][0] == {"colA": 1, "colB": "a"} # Check transformation
    assert result["result_data"][1] == {"colA": 2, "colB": "b"}

def test_get_statement_result_failed(mock_db_client_data_tools):
    # Arrange - modify fixture return for get_statement to be FAILED
    mock_get_resp = mock_db_client_data_tools.statement_execution.get_statement.return_value
    mock_get_resp.status.state = sql_service.StatementState.FAILED
    mock_get_resp.status.error = MagicMock()
    mock_get_resp.status.error.message = "SQL Error Occurred"
    mock_get_resp.result = None

    # Act
    result = get_statement_result(statement_id="stmt-123")

    # Assert
    assert result["statement_id"] == "stmt-123"
    assert result["status"] == "FAILED"
    assert result["error_message"] == "SQL Error Occurred"
    # Assert the key returned by the function
    assert result["result_data"] is None

# --- Tests for start_sql_warehouse ---
def test_start_sql_warehouse_success(mock_db_client_data_tools):
    # Arrange
    wh_id = "start-wh"
    # Act
    result = start_sql_warehouse(warehouse_id=wh_id)
    # Assert
    mock_db_client_data_tools.warehouses.start.assert_called_once_with(id=wh_id)
    # Check that the waiter was awaited
    mock_db_client_data_tools.warehouses.start.return_value.result.assert_called_once()
    assert result == {"warehouse_id": wh_id, "status": "STARTED"}

# --- Tests for stop_sql_warehouse ---
def test_stop_sql_warehouse_success(mock_db_client_data_tools):
    # Arrange
    wh_id = "stop-wh"
    # Act
    result = stop_sql_warehouse(warehouse_id=wh_id)
    # Assert
    mock_db_client_data_tools.warehouses.stop.assert_called_once_with(id=wh_id)
    # Check that the waiter was awaited
    mock_db_client_data_tools.warehouses.stop.return_value.result.assert_called_once()
    assert result == {"warehouse_id": wh_id, "status": "STOPPED"}

# Add tests for SDK errors being mapped by decorator if needed
