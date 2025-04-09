import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import serving
from databricks.sdk.service import vectorsearch as vs
from databricks.sdk.service import catalog as uc
from databricks.sdk.service import ml as mlflow_service

from databricks_mcp.tools.ml import add_to_vector_index
from databricks_mcp.tools.ml import query_model_serving_endpoint
from databricks_mcp.tools.ml import query_vector_index

from databricks_mcp.db_client import get_db_client # To mock
from databricks_mcp import error_mapping as mcp_errors
# Import error code constants
from databricks_mcp.error_mapping import CODE_INTERNAL_ERROR, CODE_INVALID_PARAMS


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_ml_tools():
    mock_client = MagicMock()
    # Add mocks for serving endpoints and vector search if they exist on the client
    mock_client.serving_endpoints = MagicMock(spec=serving.ServingEndpointsAPI)
    mock_client.vector_search_indexes = MagicMock(spec=vs.VectorSearchIndexesAPI)

    # Assign unused variable to _
    with patch('databricks_mcp.tools.ml.get_db_client', return_value=mock_client) as _:
        yield mock_client

# --- Tests for query_model_serving_endpoint ---
def test_query_model_serving_endpoint_success(mock_db_client_ml_tools):
    # Arrange
    endpoint = "my-model-endpoint"
    input_payload = {"dataframe_split": {"columns": ["col1"], "data": [[1], [2]]}}
    mock_response = MagicMock(spec=serving.QueryEndpointResponse)
    mock_response.predictions = [0.5, 0.6]
    mock_db_client_ml_tools.serving_endpoints.query.return_value = mock_response

    # Act
    result = query_model_serving_endpoint(endpoint_name=endpoint, input_data=input_payload)

    # Assert
    mock_db_client_ml_tools.serving_endpoints.query.assert_called_once_with(name=endpoint, request=input_payload)
    assert result == {"predictions": [0.5, 0.6]}

# --- Tests for add_to_vector_index ---
def test_add_to_vector_index_success(mock_db_client_ml_tools):
    # Arrange
    index = "catalog.schema.vs_index"
    pk = "id"
    docs = [{"id": 1, "text": "doc1"}, {"id": 2, "text": "doc2"}]
    mock_response = MagicMock() # Response structure might vary
    # Corrected mock response structure based on UpsertDataVectorIndexResponse
    upsert_result = MagicMock(spec=vs.UpsertDataResult)
    upsert_result.success_row_count = 2
    upsert_result.failed_primary_keys = []
    mock_response.status = vs.UpsertDataStatus.SUCCESS
    mock_response.result = upsert_result
    mock_db_client_ml_tools.vector_search_indexes.upsert_data_vector_index.return_value = mock_response # Correct method name

    # Act
    result = add_to_vector_index(index_name=index, primary_key=pk, documents=docs)

    # Assert
    mock_db_client_ml_tools.vector_search_indexes.upsert_data_vector_index.assert_called_once_with( # Correct method name
        index_name=index,
        # primary_key=pk, # API takes inputs_json, not primary key here
        inputs_json=json.dumps(docs)
    )
    assert result["status"] == "SUCCESS"
    assert result["num_added"] == 2

def test_add_to_vector_index_api_not_found(mock_db_client_ml_tools):
    # Arrange - Simulate the method not existing
    # Check if vector_search_indexes attribute exists before deleting the method
    if hasattr(mock_db_client_ml_tools, 'vector_search_indexes'):
        # Delete the specific method to simulate it not being available
        del mock_db_client_ml_tools.vector_search_indexes.upsert_data_vector_index # Correct method name
    else:
        # If the top-level attribute doesn't exist, delete the mock client attribute
        del mock_db_client_ml_tools.vector_search_indexes

    # Act & Assert
    # with pytest.raises(NotImplementedError, match="Vector Search upsert functionality not available"): # OLD
    with pytest.raises(Exception) as exc_info:
        add_to_vector_index(index_name="idx", primary_key="pk", documents=[{"pk": 1}])
    # Check wrapped exception
    assert f"[MCP Error Code {CODE_INTERNAL_ERROR}]" in str(exc_info.value)
    assert "NotImplementedError" in str(exc_info.value)
    assert "Vector Search upsert functionality not available" in str(exc_info.value)

# --- Tests for query_vector_index ---
def test_query_vector_index_success_with_vector(mock_db_client_ml_tools):
    # Arrange
    index = "catalog.schema.vs_index"
    query_vec = [0.1, 0.2, 0.3]
    cols = ["id", "text"]
    mock_manifest = MagicMock(spec=vs.ResultManifest)
    mock_manifest.as_dict.return_value = {"schema": {"columns": [{"name": "id"}, {"name": "text"}]}}
    mock_result = MagicMock(spec=vs.ResultData)
    mock_result.data_array = [[1, "doc1"], [5, "doc5"]]
    mock_response = MagicMock(spec=vs.QueryVectorIndexResponse)
    mock_response.result = mock_result
    # Ensure manifest is set on the result object within the response
    mock_response.result.manifest = mock_manifest

    mock_db_client_ml_tools.vector_search_indexes.query_index.return_value = mock_response

    # Act
    result = query_vector_index(
        index_name=index,
        columns=cols,
        query_vector=query_vec,
        num_results=5
    )

    # Assert
    mock_db_client_ml_tools.vector_search_indexes.query_index.assert_called_once_with(
        index_name=index,
        columns=cols,
        query_vector=query_vec,
        query_text=None,
        filters_json=None,
        num_results=5,
        query_type="ANN"
    )
    assert result["results"] == [[1, "doc1"], [5, "doc5"]]
    assert result["manifest"] is not None # Check manifest was returned

def test_query_vector_index_success_with_text(mock_db_client_ml_tools):
    # Arrange
    index = "catalog.schema.vs_index_with_endpoint"
    query_txt = "search for this"
    cols = ["id"]
    mock_result = MagicMock(spec=vs.ResultData)
    mock_result.data_array = [[10]]
    mock_result.manifest = None # No manifest in this mock case
    mock_response = MagicMock(spec=vs.QueryVectorIndexResponse)
    mock_response.result = mock_result
    mock_db_client_ml_tools.vector_search_indexes.query_index.return_value = mock_response

    # Act
    result = query_vector_index(
        index_name=index,
        columns=cols,
        query_text=query_txt
    )
    # Assert
    mock_db_client_ml_tools.vector_search_indexes.query_index.assert_called_once_with(
        index_name=index, columns=cols, query_vector=None, query_text=query_txt,
        filters_json=None, num_results=10, query_type="ANN"
    )
    assert result["results"] == [[10]]

def test_query_vector_index_missing_query():
    # Act & Assert
    # with pytest.raises(ValueError, match="Either 'query_vector' or 'query_text' must be provided."): # OLD
    with pytest.raises(Exception) as exc_info:
        query_vector_index(index_name="idx", columns=["id"])
    # Check wrapped exception (ValueError likely maps to Invalid Params or Internal Error)
    # Assuming Internal Error for now as it's a programming error in the caller
    assert f"[MCP Error Code {CODE_INTERNAL_ERROR}]" in str(exc_info.value)
    assert "ValueError" in str(exc_info.value)
    assert "Either 'query_vector' or 'query_text' must be provided." in str(exc_info.value)

def test_query_vector_index_both_queries():
     # Act & Assert
    # with pytest.raises(ValueError, match="Provide only one of 'query_vector' or 'query_text'."): # OLD
    with pytest.raises(Exception) as exc_info:
        query_vector_index(index_name="idx", columns=["id"], query_vector=[0.1], query_text="text")
    # Check wrapped exception
    assert f"[MCP Error Code {CODE_INTERNAL_ERROR}]" in str(exc_info.value)
    assert "ValueError" in str(exc_info.value)
    assert "Provide only one of 'query_vector' or 'query_text'." in str(exc_info.value)

def test_query_vector_index_api_not_found(mock_db_client_ml_tools):
     # Arrange - Simulate the method not existing
    if hasattr(mock_db_client_ml_tools, 'vector_search_indexes'):
        del mock_db_client_ml_tools.vector_search_indexes.query_index # Correct method name
    else:
        del mock_db_client_ml_tools.vector_search_indexes

    # Act & Assert
    # with pytest.raises(NotImplementedError, match="Vector Search query functionality not available"): # OLD
    with pytest.raises(Exception) as exc_info:
        query_vector_index(index_name="idx", columns=["id"], query_text="test")
    # Check wrapped exception
    assert f"[MCP Error Code {CODE_INTERNAL_ERROR}]" in str(exc_info.value)
    assert "NotImplementedError" in str(exc_info.value)
    assert "Vector Search query functionality not available" in str(exc_info.value)
