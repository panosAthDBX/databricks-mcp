from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import command_execution as ce
from databricks.sdk.service import jobs

from databricks_mcp.tools.workspace import execute_code
from databricks_mcp.tools.workspace import run_notebook


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_ws_tools(): # Changed fixture name
    mock_client = MagicMock()

    # Mock Jobs API run_now response and subsequent get_run
    mock_run_response = MagicMock()
    mock_run_response.run_id = 12345
    mock_run_waiter = MagicMock() # The waiter object
    mock_client.jobs.run_now.return_value = mock_run_waiter

    mock_run_details = MagicMock(spec=jobs.Run)
    mock_run_details.run_id = 12345
    mock_run_details.run_page_url = "http://example.com/run/12345"
    mock_run_details.state = MagicMock(spec=jobs.RunState)
    mock_run_details.state.life_cycle_state = jobs.RunLifeCycleState.TERMINATED
    mock_run_details.state.result_state = jobs.RunResultState.SUCCESS
    # Make run_now().result() return the details (simulating wait)
    mock_run_waiter.result.return_value = mock_run_details
    # Make get_run return the details as well (needed by the implementation)
    mock_client.jobs.get_run.return_value = mock_run_details

    # Mock Command Execution API execute response
    mock_cmd_response = MagicMock(spec=ce.Command)
    mock_cmd_response.id = "cmd-abc"
    mock_cmd_response.status = ce.CommandStatus.FINISHED
    mock_cmd_response.results = MagicMock(spec=ce.CommandResults)
    mock_cmd_response.results.result_type = ce.ResultType.TEXT
    mock_cmd_response.results.data = "Command output"
    # Make execute return the command response after waiting
    mock_execute_waiter = MagicMock()
    mock_execute_waiter.result.return_value = mock_cmd_response
    mock_client.command_execution.execute.return_value = mock_execute_waiter

    # Assign unused variable to _
    with patch('databricks_mcp.tools.workspace.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for run_notebook ---

def test_run_notebook_success_existing_cluster(mock_db_client_ws_tools):
    # Arrange
    notebook = "/Users/test/my_nb"
    cluster = "cluster-1"
    params = {"param1": "value1"}

    # Act
    result = run_notebook(notebook_path=notebook, cluster_id=cluster, parameters=params)

    # Assert
    mock_db_client_ws_tools.jobs.run_now.assert_called_once_with(
        run_name=f"MCP Run: {notebook}",
        tasks=[{"notebook_task": {"notebook_path": notebook, "base_parameters": params}}],
        existing_cluster_id=cluster
    )
    mock_db_client_ws_tools.jobs.run_now.return_value.result.assert_called_once() # Check wait
    mock_db_client_ws_tools.jobs.get_run.assert_called_once_with(run_id=12345)
    assert result["run_id"] == 12345
    assert result["status"] == "TERMINATED"
    assert result["result_state"] == "SUCCESS"
    assert result["run_page_url"] == "http://example.com/run/12345"

def test_run_notebook_success_no_cluster(mock_db_client_ws_tools):
    # Arrange
    notebook = "/Users/test/other_nb"
    # Act - No cluster_id provided
    result = run_notebook(notebook_path=notebook, parameters=None)

    # Assert - Should call run_now without existing_cluster_id
    mock_db_client_ws_tools.jobs.run_now.assert_called_once_with(
        run_name=f"MCP Run: {notebook}",
        tasks=[{"notebook_task": {"notebook_path": notebook, "base_parameters": {}}}],
        # No cluster spec passed if cluster_id is None and no default new_cluster defined
    )
    assert result["run_id"] == 12345 # Still returns run details

def test_run_notebook_failed_run(mock_db_client_ws_tools):
    # Arrange - Modify the mock get_run response for failure
    mock_run_details_failed = MagicMock(spec=jobs.Run)
    mock_run_details_failed.run_id = 67890
    mock_run_details_failed.run_page_url = "http://example.com/run/67890"
    mock_run_details_failed.state = MagicMock(spec=jobs.RunState)
    mock_run_details_failed.state.life_cycle_state = jobs.RunLifeCycleState.TERMINATED
    mock_run_details_failed.state.result_state = jobs.RunResultState.FAILED
    mock_run_details_failed.state.state_message = "Notebook failed"

    # Simulate run_now returning the new run_id, then get_run returning failed state
    mock_run_response_failed = MagicMock()
    mock_run_response_failed.run_id = 67890
    mock_run_waiter_failed = MagicMock() # Waiter for the failed run
    # Make .result() return the failed details
    mock_run_waiter_failed.result.return_value = mock_run_details_failed
    mock_db_client_ws_tools.jobs.run_now.return_value = mock_run_waiter_failed
    mock_db_client_ws_tools.jobs.get_run.return_value = mock_run_details_failed

    # Act
    result = run_notebook(notebook_path="/fail", cluster_id="c1")

    # Assert
    assert result["run_id"] == 67890
    assert result["status"] == "TERMINATED"
    assert result["result_state"] == "FAILED"


# --- Tests for execute_code ---

def test_execute_code_success(mock_db_client_ws_tools):
    # Arrange
    code = "print('hello')"
    lang = "python"
    cluster = "cluster-exec"

    # Act
    result = execute_code(code=code, language=lang, cluster_id=cluster)

    # Assert
    mock_db_client_ws_tools.command_execution.execute.assert_called_once_with(
        language=lang,
        cluster_id=cluster,
        command=code
    )
    mock_db_client_ws_tools.command_execution.execute.return_value.result.assert_called_once() # Check wait
    assert result["command_id"] == "cmd-abc"
    assert result["status"] == "CommandStatus.FINISHED"
    assert result["result_type"] == "ResultType.TEXT"
    assert result["result_data"] == "Command output"


def test_execute_code_error_result(mock_db_client_ws_tools):
    # Arrange - Modify mock execute response for error
    mock_cmd_response_err = MagicMock(spec=ce.Command)
    mock_cmd_response_err.id = "cmd-err"
    mock_cmd_response_err.status = ce.CommandStatus.ERROR
    mock_cmd_response_err.results = MagicMock(spec=ce.CommandResults)
    mock_cmd_response_err.results.result_type = ce.ResultType.ERROR
    mock_cmd_response_err.results.cause = "Traceback...\nNameError: name 'x' is not defined"
    mock_cmd_response_err.results.data = None # No data on error typically

    mock_execute_waiter_err = MagicMock()
    mock_execute_waiter_err.result.return_value = mock_cmd_response_err
    mock_db_client_ws_tools.command_execution.execute.return_value = mock_execute_waiter_err

    # Act
    result = execute_code(code="print(x)", language="python", cluster_id="c-err")

    # Assert
    assert result["command_id"] == "cmd-err"
    assert result["status"] == "CommandStatus.ERROR"
    assert result["result_type"] == "ResultType.ERROR"
    assert "NameError" in result["result_data"] # Contains the error cause


# Add tests for SDK errors being mapped by decorator if needed, similar to compute tests
