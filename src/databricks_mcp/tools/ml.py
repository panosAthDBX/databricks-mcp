import json

import structlog

# Import the mcp instance from app.py
from ..app import mcp
from databricks.sdk.service import serving as serving_endpoints, vectorsearch as vector_search

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.tool(
    name="databricks-ml-query_model_serving_endpoint",
    description="Queries a Databricks Model Serving endpoint with the provided input data.",
)
def query_model_serving_endpoint(endpoint_name: str, input_data: dict | list) -> dict:
    """
    Sends data to a Databricks Model Serving endpoint and gets predictions.
    REQ-ML-TOOL-01

    Args:
        endpoint_name: The name of the deployed Model Serving endpoint.
        input_data: Input data payload (dict or list). Format depends on the model.
    """
    db = get_db_client()
    log.info("Querying Model Serving endpoint", endpoint_name=endpoint_name)

    response = db.serving_endpoints.query(name=endpoint_name, request=input_data)

    predictions = response.predictions if hasattr(response, 'predictions') else response.as_dict()

    log.info("Successfully queried endpoint", endpoint_name=endpoint_name)
    return {"predictions": predictions}


# --- Vector Search Tools ---
# Note: These assume the Vector Search Index exists and is accessible.

@map_databricks_errors
@mcp.tool(
    name="databricks-vs-add_to_index",
    description="Adds or updates documents (as dictionaries) in a Databricks Vector Search index.",
)
def add_to_vector_index(index_name: str, primary_key: str, documents: list[dict]) -> dict:
    """
    Add/update documents in a Databricks Vector Search index.
    REQ-ML-TOOL-02

    Args:
        index_name: Full name of the Vector Search index (e.g., 'catalog.schema.my_index').
        primary_key: Name of the primary key column in the documents.
        documents: List of dictionaries representing documents to add/update.
    """
    db = get_db_client()
    log.info("Adding/updating documents in Vector Search index", index_name=index_name, doc_count=len(documents))

    try:
        response = db.vector_search_indexes.upsert_data_vector_index(
            index_name=index_name,
            inputs_json=json.dumps(documents) # API likely expects JSON string
        )
        # Check response structure based on SDK
        status = "UNKNOWN"
        num_added = 0
        summary = None
        if hasattr(response, 'result') and response.result:
            if hasattr(response, 'status'):
                status = str(response.status.value) if response.status else "UNKNOWN"
            if hasattr(response.result, 'success_row_count'):
                num_added = response.result.success_row_count or 0
            # Reconstruct a summary similar to previous expectation if needed for return value
            summary = { "status": status, "success_row_count": num_added }
            if hasattr(response.result, 'failed_primary_keys') and response.result.failed_primary_keys:
                summary["failed_primary_keys"] = response.result.failed_primary_keys

        log.info("Upsert data result", index_name=index_name, status=status, num_added=num_added)
        return {"status": status, "num_added": num_added, "response_summary": summary}

    except AttributeError:
         log.error("Vector Search client/method (db.vector_search_indexes.upsert_data_vector_index) not found in SDK. Check SDK version/API.")
         raise NotImplementedError("Vector Search upsert functionality not available in current SDK setup.")
    except ImportError:
         log.error("Vector Search requires additional dependencies. Try `pip install databricks-vectorsearch`")
         raise NotImplementedError("Vector Search requires additional dependencies.")


@map_databricks_errors
@mcp.tool(
    name="databricks-vs-query_index",
    description="Queries a Databricks Vector Search index to find similar documents.",
)
def query_vector_index(
    index_name: str,
    columns: list[str],
    query_vector: list[float] | None = None,
    query_text: str | None = None,
    num_results: int = 10,
    filters_json: str | None = None,
    query_type: str = "ANN"
) -> dict:
    """
    Query a Databricks Vector Search index.
    REQ-ML-TOOL-03
    Requires either query_vector or query_text.

    Args:
        index_name: Full name of the Vector Search index.
        columns: List of column names to include in the results.
        query_vector: Optional query vector (list of floats).
        query_text: Optional query text (will be embedded by Databricks).
        num_results: Number of results to return (default 10).
        filters_json: Optional JSON string for filtering results.
        query_type: Type of query, 'ANN' or 'HYBRID' (default 'ANN').
    """
    if not query_vector and not query_text:
        raise ValueError("Either 'query_vector' or 'query_text' must be provided.")
    if query_vector and query_text:
        raise ValueError("Provide only one of 'query_vector' or 'query_text'.")

    db = get_db_client()
    log.info("Querying Vector Search index", index_name=index_name, num_results=num_results, has_vector=bool(query_vector), has_text=bool(query_text))

    try:
        response = db.vector_search_indexes.query_index(
            index_name=index_name,
            columns=columns,
            query_vector=query_vector,
            query_text=query_text,
            filters_json=filters_json,
            num_results=num_results,
            query_type=query_type
        )

        result_data = []
        if response.result and response.result.data_array:
            result_data = response.result.data_array # Data is typically list of lists/dicts

        log.info("Vector Search query successful", index_name=index_name, results_count=len(result_data))
        return {
            "results": result_data,
            "manifest": response.result.manifest.as_dict() if response.result and response.result.manifest else None
        }
    except AttributeError:
         log.error("Vector Search client/method (db.vector_search_indexes.query_index) not found in SDK. Check SDK version/API.")
         raise NotImplementedError("Vector Search query functionality not available in current SDK setup.")
    except ImportError:
         log.error("Vector Search requires additional dependencies. Try `pip install databricks-vectorsearch`")
         raise NotImplementedError("Vector Search requires additional dependencies.")
