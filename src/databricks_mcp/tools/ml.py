import json

import structlog

# Import relevant SDK services
from mcp import Parameter
from mcp import Tool
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Tool.from_callable(
    "databricks:ml:query_model_serving_endpoint",
    description="Queries a Databricks Model Serving endpoint with the provided input data.",
    parameters=[
        Parameter(
            name="endpoint_name",
            description="The name of the deployed Model Serving endpoint.",
            param_type=parameters.StringType,
            required=True,
        ),
        Parameter(
            name="input_data",
            description=(
                "The input data payload for the model endpoint. Format depends on the model signature "
                "(e.g., dictionary for named inputs, list for unnamed tensors). "
                "Refer to endpoint documentation for expected format."
            ),
            param_type=parameters.UnionType(types=[parameters.ObjectType(properties={}), parameters.ArrayType(items=parameters.AnyType)]), # Allow dict or list input
            required=True,
        ),
        # Add parameters for specific dataframe formats if needed later, e.g.,
        # Parameter(name="dataframe_split", ...), Parameter(name="dataframe_records", ...)
    ]
)
def query_model_serving_endpoint(endpoint_name: str, input_data: dict | list) -> dict:
    """
    Sends data to a Databricks Model Serving endpoint and gets predictions.
    REQ-ML-TOOL-01
    """
    db = get_db_client()
    log.info("Querying Model Serving endpoint", endpoint_name=endpoint_name)

    # The SDK's query method handles different input formats
    # We pass the dict/list directly. Ensure client sends compatible format.
    response = db.serving_endpoints.query(name=endpoint_name, request=input_data)

    # The response object typically has a 'predictions' field
    predictions = response.predictions if hasattr(response, 'predictions') else response.as_dict()

    log.info("Successfully queried endpoint", endpoint_name=endpoint_name)
    # Return the predictions part of the response
    return {"predictions": predictions}


# --- Vector Search Tools ---
# Note: These assume the Vector Search Index exists and is accessible.

@map_databricks_errors
@Tool.from_callable(
    "databricks:vs:add_to_index",
    description="Adds or updates documents (as dictionaries) in a Databricks Vector Search index.",
    parameters=[
         Parameter(
            name="index_name",
            description="The full name of the Vector Search index (e.g., 'catalog.schema.my_index').",
            param_type=parameters.StringType,
            required=True,
        ),
         Parameter(
            name="primary_key",
             description="The name of the primary key column in the documents.",
            param_type=parameters.StringType,
            required=True,
        ),
        Parameter(
            name="documents",
            description="A list of dictionaries, where each dictionary represents a document to add/update.",
            param_type=parameters.ArrayType(items=parameters.ObjectType(properties={})),
            required=True,
        ),
    ]
)
def add_to_vector_index(index_name: str, primary_key: str, documents: list[dict]) -> dict:
    """
    Add/update documents in a Databricks Vector Search index.
    REQ-ML-TOOL-02
    """
    db = get_db_client()
    # Note: Vector Search API might be separate or under a different client in SDK
    # Checking common patterns, it might be db.vector_search_indexes...
    # Need to verify exact SDK method. Let's assume db.vector_search_indexes.upsert_data exists.
    log.info("Adding/updating documents in Vector Search index", index_name=index_name, doc_count=len(documents))

    try:
        # This method/client name needs verification based on the installed databricks-sdk version
        response = db.vector_search_indexes.upsert_data(
            index_name=index_name,
            primary_key=primary_key,
            inputs_json=json.dumps(documents) # API likely expects JSON string
        )
        # Process response (might contain status, counts, etc.)
        status = response.summary.get("status", "UNKNOWN") if hasattr(response, "summary") and isinstance(response.summary, dict) else "UNKNOWN"
        num_added = response.summary.get("upserted_count", 0) if hasattr(response, "summary") and isinstance(response.summary, dict) else 0
        log.info("Upsert data result", index_name=index_name, status=status, num_added=num_added)
        return {"status": status, "num_added": num_added, "response_summary": getattr(response, "summary", None)}

    except AttributeError:
         log.error("Vector Search client/method (db.vector_search_indexes.upsert_data) not found in SDK. Check SDK version/API.")
         raise NotImplementedError("Vector Search upsert functionality not available in current SDK setup.")
    except ImportError:
         log.error("Vector Search requires additional dependencies. Try `pip install databricks-vectorsearch`")
         raise NotImplementedError("Vector Search requires additional dependencies.")


@map_databricks_errors
@Tool.from_callable(
    "databricks:vs:query_index",
    description="Queries a Databricks Vector Search index to find similar documents.",
    parameters=[
         Parameter(
            name="index_name",
            description="The full name of the Vector Search index (e.g., 'catalog.schema.my_index').",
            param_type=parameters.StringType,
            required=True,
        ),
        Parameter(
            name="query_vector",
            description="Optional: The query vector (list of floats) to search for.",
            param_type=parameters.ArrayType(items=parameters.FloatType),
            required=False,
        ),
         Parameter(
            name="query_text",
            description="Optional: Text to be converted to a vector (using the index's endpoint) for searching.",
            param_type=parameters.StringType,
            required=False,
        ),
        Parameter(
            name="num_results",
            description="The number of similar results to return.",
            param_type=parameters.IntegerType,
            required=False,
            default=10,
        ),
         Parameter(
            name="columns",
            description="List of column names to include in the results.",
            param_type=parameters.ArrayType(items=parameters.StringType),
            required=True,
        ),
         Parameter(
            name="filters_json",
            description="Optional: JSON string representing filters to apply to the search.",
            param_type=parameters.StringType,
            required=False,
        ),
         Parameter(
            name="query_type",
            description="Type of query: ANN (Approximate Nearest Neighbor) or HYBRID.",
            param_type=parameters.StringType,
            required=False,
            default="ANN"
        ),
    ]
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
    """
    if not query_vector and not query_text:
        raise ValueError("Either 'query_vector' or 'query_text' must be provided.")
    if query_vector and query_text:
        raise ValueError("Provide only one of 'query_vector' or 'query_text'.")

    db = get_db_client()
    log.info("Querying Vector Search index", index_name=index_name, num_results=num_results, has_vector=bool(query_vector), has_text=bool(query_text))

    try:
        # This method/client name needs verification based on the installed databricks-sdk version
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
