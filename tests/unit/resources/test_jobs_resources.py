from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import jobs as jobs_service

from databricks_mcp.resources.jobs import get_job_details
from databricks_mcp.resources.jobs import list_job_runs
from databricks_mcp.resources.jobs import list_jobs


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_jobs():
    mock_client = MagicMock()
    with patch('databricks_mcp.resources.jobs.get_db_client', return_value=mock_client) as mock_get:
        yield mock_client

# --- Tests for list_jobs ---
def test_list_jobs_success(mock_db_client_jobs):
    # Arrange
    job1_settings = MagicMock(spec=jobs_service.JobSettings)
    job1_settings.name = "Job One"
    job1_settings.schedule = MagicMock(spec=jobs_service.CronSchedule)
    job1_settings.schedule.quartz_cron_expression = "0 0 1 * * ?"
    job1_settings.schedule.timezone_id = "UTC"

    job1 = MagicMock(spec=jobs_service.Job)
    job1.job_id = 101
    job1.creator_user_name = "user1"
    job1.created_time = 1678886400000
    job1.settings = job1_settings

    mock_db_client_jobs.jobs.list.return_value = [job1]

    # Act
    result = list_jobs(limit=10)

    # Assert
    mock_db_client_jobs.jobs.list.assert_called_once_with(name=None, limit=10)
    assert len(result) == 1
    assert result[0] == {
        "job_id": 101,
        "name": "Job One",
        "creator_user_name": "user1",
        "schedule_quartz_expr": "0 0 1 * * ?",
        "schedule_timezone": "UTC",
        "created_time": 1678886400000,
    }

def test_list_jobs_with_filter(mock_db_client_jobs):
    mock_db_client_jobs.jobs.list.return_value = [] # Assume filter returns none
    list_jobs(name_filter="SpecificJob", limit=5)
    mock_db_client_jobs.jobs.list.assert_called_once_with(name="SpecificJob", limit=5)


# --- Tests for get_job_details ---
def test_get_job_details_success(mock_db_client_jobs):
    # Arrange
    mock_settings_dict = {"name": "Detailed Job", "tasks": [{"task_key": "A", "notebook_task": {"notebook_path": "/nb"}}]}
    mock_settings = MagicMock(spec=jobs_service.JobSettings)
    mock_settings.as_dict.return_value = mock_settings_dict # Mock the dict conversion

    mock_job = MagicMock(spec=jobs_service.Job)
    mock_job.job_id = 202
    mock_job.creator_user_name = "creator"
    mock_job.created_time = 1678880000000
    mock_job.run_as_user_name = "service_principal"
    mock_job.settings = mock_settings

    mock_db_client_jobs.jobs.get.return_value = mock_job

    # Act
    result = get_job_details(job_id=202)

    # Assert
    mock_db_client_jobs.jobs.get.assert_called_once_with(job_id=202)
    assert result == {
        "job_id": 202,
        "creator_user_name": "creator",
        "created_time": 1678880000000,
        "run_as_user_name": "service_principal",
        "settings": mock_settings_dict,
    }

# --- Tests for list_job_runs ---
def test_list_job_runs_success(mock_db_client_jobs):
    # Arrange
    run1_state = MagicMock(spec=jobs_service.RunState)
    run1_state.life_cycle_state = jobs_service.RunLifeCycleState.TERMINATED
    run1_state.result_state = jobs_service.RunResultState.SUCCESS
    run1_state.state_message = "Finished"

    run1 = MagicMock(spec=jobs_service.Run)
    run1.run_id = 5001
    run1.job_id = 303
    run1.start_time = 1678890000000
    run1.end_time = 1678890100000
    run1.execution_duration = 100000
    run1.state = run1_state
    run1.run_page_url = "http://..."
    run1.trigger = jobs_service.TriggerType.PERIODIC

    mock_db_client_jobs.jobs.list_runs.return_value = [run1]

    # Act
    result = list_job_runs(job_id=303, limit=10)

    # Assert
    mock_db_client_jobs.jobs.list_runs.assert_called_once_with(job_id=303, limit=10)
    assert len(result) == 1
    assert result[0] == {
        "run_id": 5001,
        "job_id": 303,
        "start_time": 1678890000000,
        "end_time": 1678890100000,
        "duration": 100000,
        "state_life_cycle": "TERMINATED",
        "state_result": "SUCCESS",
        "state_message": "Finished",
        "run_page_url": "http://...",
        "trigger_type": "PERIODIC",
    }

def test_list_job_runs_with_status_filter(mock_db_client_jobs):
     # Arrange
    run_term_succ = MagicMock(spec=jobs_service.Run); run_term_succ.run_id = 1
    run_term_succ.state = MagicMock(); run_term_succ.state.life_cycle_state = jobs_service.RunLifeCycleState.TERMINATED
    run_term_succ.state.result_state = jobs_service.RunResultState.SUCCESS

    run_term_fail = MagicMock(spec=jobs_service.Run); run_term_fail.run_id = 2
    run_term_fail.state = MagicMock(); run_term_fail.state.life_cycle_state = jobs_service.RunLifeCycleState.TERMINATED
    run_term_fail.state.result_state = jobs_service.RunResultState.FAILED

    run_running = MagicMock(spec=jobs_service.Run); run_running.run_id = 3
    run_running.state = MagicMock(); run_running.state.life_cycle_state = jobs_service.RunLifeCycleState.RUNNING
    run_running.state.result_state = None # No result state yet

    # Mock SDK returning all runs (filtering happens in our code for this test)
    mock_db_client_jobs.jobs.list_runs.return_value = [run_term_succ, run_term_fail, run_running]

    # Act
    result = list_job_runs(job_id=404, status_filter="TERMINATED") # Filter for TERMINATED

     # Assert
    mock_db_client_jobs.jobs.list_runs.assert_called_once_with(job_id=404, limit=25) # Default limit
    assert len(result) == 2 # Only terminated runs should be returned
    assert result[0]["run_id"] == 1
    assert result[0]["state_life_cycle"] == "TERMINATED"
    assert result[1]["run_id"] == 2
    assert result[1]["state_life_cycle"] == "TERMINATED"
