import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from databricks.sdk.service import serving
from databricks.sdk.service import vectorsearch as vs

from databricks_mcp.tools.ml import add_to_vector_index
from databricks_mcp.tools.ml import query_model_serving_endpoint
from databricks_mcp.tools.ml import query_vector_index

from databricks_mcp.db_client import get_db_client # To mock
from mcp import errors as mcp_errors


# Mock the get_db_client function
@pytest.fixture(autouse=True)
def mock_db_client_ml_tools():
    mock_client = MagicMock()
    # Add mocks for serving endpoints and vector search if they exist on the client
    mock_client.serving_endpoints = MagicMock(spec=serving.ServingEndpointsAPI)
    mock_client.vector_search_indexes = MagicMock(spec=vs.VectorSearchIndexesAPI) # Assuming this structure

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
    mock_response.summary = {"status": "SUCCESS", "upserted_count": 2}
    mock_db_client_ml_tools.vector_search_indexes.upsert_data.return_value = mock_response

    # Act
    result = add_to_vector_index(index_name=index, primary_key=pk, documents=docs)

    # Assert
    mock_db_client_ml_tools.vector_search_indexes.upsert_data.assert_called_once_with(
        index_name=index, primary_key=pk, inputs_json=json.dumps(docs)
    )
    assert result["status"] == "SUCCESS"
    assert result["num_added"] == 2

def test_add_to_vector_index_api_not_found(mock_db_client_ml_tools):
    # Arrange - Simulate the method not existing
    del mock_db_client_ml_tools.vector_search_indexes # Or make upsert_data raise AttributeError
    # mock_db_client_ml_tools.vector_search_indexes.upsert_data.side_effect = AttributeError

    # Act & Assert
    with pytest.raises(NotImplementedError, match="Vector Search upsert functionality not available"):
        add_to_vector_index(index_name="idx", primary_key="pk", documents=[{"pk": 1}])

# --- Tests for query_vector_index ---
def test_query_vector_index_success_with_vector(mock_db_client_ml_tools):
    # Arrange
    index = "catalog.schema.vs_index"
    query_vec = [0.1, 0.2, 0.3]
    cols = ["id", "text"]
    mock_manifest = MagicMock()
    mock_manifest.as_dict.return_value = {"schema": {"columns": [{"name": "id"}, {"name": "text"}]}}
    mock_result = MagicMock()
    mock_result.data_array = [[1, "doc1"], [5, "doc5"]] # Example result format
    mock_result.manifest = mock_manifest
    mock_response = MagicMock(spec=vs.QueryVectorIndexResponse)
    mock_response.result = mock_result

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
    assert result["manifest"] is not None

def test_query_vector_index_success_with_text(mock_db_client_ml_tools):
    # Arrange
    index = "catalog.schema.vs_index_with_endpoint" # Assumes index linked to endpoint
    query_txt = "search for this"
    cols = ["id"]
    mock_result = MagicMock()
    mock_result.data_array = [[10]]
    mock_result.manifest = None
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
    with pytest.raises(ValueError, match="Either 'query_vector' or 'query_text' must be provided."):
        query_vector_index(index_name="idx", columns=["id"])

def test_query_vector_index_both_queries():
     # Act & Assert
    with pytest.raises(ValueError, match="Provide only one of 'query_vector' or 'query_text'."):
        query_vector_index(index_name="idx", columns=["id"], query_vector=[0.1], query_text="text")

def test_query_vector_index_api_not_found(mock_db_client_ml_tools):
     # Arrange - Simulate the method not existing
    del mock_db_client_ml_tools.vector_search_indexes
    # Act & Assert
    with pytest.raises(NotImplementedError, match="Vector Search query functionality not available"):
        query_vector_index(index_name="idx", columns=["id"], query_text="test")
