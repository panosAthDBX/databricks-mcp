from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import jobs as jobs_service

from databricks_mcp.tools.jobs import run_job_now
from databricks_mcp.db_client import get_db_client # To mock
from mcp import errors as mcp_errors


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_jobs_tools():
    mock_client = MagicMock()

    # Mock run_now response and subsequent get_run
    mock_run_response = MagicMock()
    mock_run_response.run_id = 9876
    # The waiter object returned by run_now
    mock_run_waiter = MagicMock()
    mock_client.jobs.run_now.return_value = mock_run_waiter

    mock_run_details = MagicMock(spec=jobs_service.Run)
    mock_run_details.run_id = 9876
    mock_run_details.run_page_url = "http://example.com/run/9876"
    mock_run_details.state = MagicMock(spec=jobs_service.RunState)
    mock_run_details.state.life_cycle_state = jobs_service.RunLifeCycleState.TERMINATED
    mock_run_details.state.result_state = jobs_service.RunResultState.SUCCESS
    # Make run_now().result() return the details (simulating wait)
    mock_run_waiter.result.return_value = mock_run_details
    # Make get_run also return the details
    mock_client.jobs.get_run.return_value = mock_run_details

    # Assign unused variable to _
    with patch('databricks_mcp.tools.jobs.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for run_job_now ---
def test_run_job_now_success_no_params(mock_db_client_jobs_tools):
    # Act
    result = run_job_now(job_id=555)

    # Assert
    mock_db_client_jobs_tools.jobs.run_now.assert_called_once_with(
        job_id=555,
        notebook_params=None,
        python_params=None,
        jar_params=None,
        spark_submit_params=None
    )
    mock_db_client_jobs_tools.jobs.run_now.return_value.result.assert_called_once() # Check wait
    mock_db_client_jobs_tools.jobs.get_run.assert_called_once_with(run_id=9876)
    assert result["run_id"] == 9876
    assert result["status"] == "TERMINATED"
    assert result["result_state"] == "SUCCESS"

def test_run_job_now_success_with_params(mock_db_client_jobs_tools):
    # Arrange
    nb_params = {"p1": "v1"}
    py_params = ["arg1", "arg2"]

    # Act
    result = run_job_now(
        job_id=556,
        notebook_params=nb_params,
        python_params=py_params
    )

    # Assert
    mock_db_client_jobs_tools.jobs.run_now.assert_called_once_with(
        job_id=556,
        notebook_params=nb_params,
        python_params=py_params,
        jar_params=None,
        spark_submit_params=None
    )
    assert result["run_id"] == 9876 # Assuming same mock run_id for simplicity

def test_run_job_now_failure(mock_db_client_jobs_tools):
    # Arrange - Modify the mock get_run response for failure
    mock_run_details_failed = MagicMock(spec=jobs_service.Run)
    mock_run_details_failed.run_id = 9877
    mock_run_details_failed.run_page_url = "http://example.com/run/9877"
    mock_run_details_failed.state = MagicMock(spec=jobs_service.RunState)
    mock_run_details_failed.state.life_cycle_state = jobs_service.RunLifeCycleState.TERMINATED
    mock_run_details_failed.state.result_state = jobs_service.RunResultState.FAILED
    mock_run_details_failed.state.state_message = "Job task failed"

    # Simulate run_now returning the new run_id, then get_run returning failed state
    mock_run_response_failed = MagicMock()
    mock_run_response_failed.run_id = 9877
    mock_run_waiter_failed = MagicMock()
    # Make .result() return the failed details
    mock_run_waiter_failed.result.return_value = mock_run_details_failed
    mock_db_client_jobs_tools.jobs.run_now.return_value = mock_run_waiter_failed
    mock_db_client_jobs_tools.jobs.get_run.return_value = mock_run_details_failed

    # Act
    result = run_job_now(job_id=557)

    # Assert
    assert result["run_id"] == 9877
    assert result["status"] == "TERMINATED"
    assert result["result_state"] == "FAILED"

# Add tests for SDK error mapping if needed
