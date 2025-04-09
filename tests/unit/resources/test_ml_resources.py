from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import ml as mlflow_service

from databricks_mcp.resources.ml import get_mlflow_run_details
from databricks_mcp.resources.ml import get_model_version_details
from databricks_mcp.resources.ml import list_mlflow_experiments
from databricks_mcp.resources.ml import list_mlflow_runs
from databricks_mcp.resources.ml import list_registered_models


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_ml():
    mock_client = MagicMock()
    with patch('databricks_mcp.resources.ml.get_db_client', return_value=mock_client) as mock_get:
        yield mock_client

# --- Tests for list_mlflow_experiments ---
def test_list_mlflow_experiments_success(mock_db_client_ml):
    # Arrange
    exp1 = MagicMock()
    exp1.experiment_id = "exp1"; exp1.name = "Exp One"; exp1.artifact_location = "dbfs:/..."
    exp1.lifecycle_stage = "active"; exp1.creation_time = 1; exp1.last_update_time = 2

    mock_response = MagicMock()
    mock_response.experiments = [exp1]
    mock_db_client_ml.experiments.list_experiments.return_value = mock_response

    # Act
    result = list_mlflow_experiments(max_results=50)

    # Assert
    mock_db_client_ml.experiments.list_experiments.assert_called_once_with(max_results=50)
    assert len(result) == 1
    assert result[0]["experiment_id"] == "exp1"
    assert result[0]["name"] == "Exp One"

# --- Tests for list_mlflow_runs ---
def test_list_mlflow_runs_success(mock_db_client_ml):
    # Arrange
    run_info = MagicMock()
    run_info.run_id = "run1"; run_info.experiment_id = "exp1"; run_info.user_id = "user"
    run_info.status = "FINISHED"; run_info.start_time = 10; run_info.end_time = 20
    run_info.artifact_uri = "dbfs:/..."; run_info.lifecycle_stage = "active"
    run = MagicMock()
    run.info = run_info

    mock_response = MagicMock()
    mock_response.runs = [run]
    mock_db_client_ml.experiments.search_runs.return_value = mock_response

    # Act
    result = list_mlflow_runs(experiment_id="exp1", filter_string="metrics.acc > 0.9", max_results=10)

    # Assert
    mock_db_client_ml.experiments.search_runs.assert_called_once_with(
        experiment_ids=["exp1"], filter="metrics.acc > 0.9", max_results=10
    )
    assert len(result) == 1
    assert result[0]["run_id"] == "run1"
    assert result[0]["status"] == "FINISHED"

# --- Tests for get_mlflow_run_details ---
def test_get_mlflow_run_details_success(mock_db_client_ml):
     # Arrange
    run_info = MagicMock()
    run_info.run_id="run-detail"; run_info.experiment_id="exp-d"; run_info.user_id="ud"
    run_info.status="FINISHED"; run_info.start_time=1; run_info.end_time=2
    run_info.artifact_uri="art/uri"; run_info.lifecycle_stage="active"

    param = MagicMock()
    param.key="p1"; param.value="v1"
    metric = MagicMock()
    metric.key="m1"; metric.value=0.99
    tag = MagicMock()
    tag.key="t1"; tag.value="tv1"
    run_data = MagicMock()
    run_data.params=[param]; run_data.metrics=[metric]; run_data.tags=[tag]

    run = MagicMock()
    run.info=run_info; run.data=run_data

    mock_response = MagicMock()
    mock_response.run = run
    mock_db_client_ml.experiments.get_run.return_value = mock_response

    # Act
    result = get_mlflow_run_details(run_id="run-detail")

    # Assert
    mock_db_client_ml.experiments.get_run.assert_called_once_with(run_id="run-detail")
    assert result["run_id"] == "run-detail"
    assert result["params"] == {"p1": "v1"}
    assert result["metrics"] == {"m1": 0.99}
    assert result["tags"] == {"t1": "tv1"}

# --- Tests for list_registered_models ---
def test_list_registered_models_success(mock_db_client_ml):
    # Arrange
    latest_ver = MagicMock()
    latest_ver.name = "Model A"; latest_ver.version = "2"; latest_ver.current_stage = "Production"; latest_ver.status = "READY"

    model1 = MagicMock()
    model1.name = "Model A"; model1.creation_timestamp = 1; model1.last_updated_timestamp = 2
    model1.user_id = "u"; model1.description = "Desc A"; model1.latest_versions = [latest_ver]

    mock_response = MagicMock()
    mock_response.registered_models = [model1]
    mock_db_client_ml.model_registry.search_registered_models.return_value = mock_response

    # Act
    result = list_registered_models(filter_string="name='Model A'")

    # Assert
    mock_db_client_ml.model_registry.search_registered_models.assert_called_once_with(filter="name='Model A'", max_results=100)
    assert len(result) == 1
    assert result[0]["name"] == "Model A"
    assert len(result[0]["latest_versions"]) == 1
    assert result[0]["latest_versions"][0]["version"] == "2"
    assert result[0]["latest_versions"][0]["current_stage"] == "Production"


# --- Tests for get_model_version_details ---
def test_get_model_version_details_success(mock_db_client_ml):
    # Arrange
    tag = MagicMock()
    tag.key="tag1"; tag.value="val1"
    version_info = MagicMock()
    version_info.name="ModelB"; version_info.version="1"; version_info.creation_timestamp=10
    version_info.last_updated_timestamp=11; version_info.user_id="u2"
    version_info.current_stage="Staging"; version_info.description="Version 1"
    version_info.source="source/path"; version_info.run_id="run-abc"; version_info.status="READY"
    version_info.status_message="OK"; version_info.tags=[tag]

    mock_response = MagicMock()
    mock_response.model_version = version_info
    mock_db_client_ml.model_registry.get_model_version.return_value = mock_response

    # Act
    result = get_model_version_details(model_name="ModelB", version="1")

    # Assert
    mock_db_client_ml.model_registry.get_model_version.assert_called_once_with(name="ModelB", version="1")
    assert result["name"] == "ModelB"
    assert result["version"] == "1"
    assert result["current_stage"] == "Staging"
    assert result["run_id"] == "run-abc"
    assert result["tags"] == {"tag1": "val1"}
