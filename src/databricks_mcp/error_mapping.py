import functools

import structlog
from databricks.sdk.errors import BadRequest
from databricks.sdk.errors import DatabricksError
from databricks.sdk.errors import NotFound
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.errors import RateLimitExceeded
from databricks.sdk.errors import ResourceDoesNotExist
from mcp import errors as mcp_errors  # Assuming mcp library provides error definitions

log = structlog.get_logger(__name__)

# Map specific Databricks exceptions to MCP error codes.
# We use standard JSON-RPC codes where applicable, or generic server errors.
# The mcp library might offer more specific error types/codes.
ERROR_MAP = {
    NotFound: mcp_errors.ErrorCode.SERVER_ERROR_RESOURCE_NOT_FOUND, # Or ResourceDoesNotExist
    ResourceDoesNotExist: mcp_errors.ErrorCode.SERVER_ERROR_RESOURCE_NOT_FOUND,
    PermissionDenied: mcp_errors.ErrorCode.SERVER_ERROR_PERMISSION_DENIED,
    BadRequest: mcp_errors.ErrorCode.INVALID_PARAMS, # Often indicates bad input from client
    RateLimitExceeded: mcp_errors.ErrorCode.SERVER_ERROR_RATE_LIMIT,
    # Catch broader DatabricksError last as a fallback
    DatabricksError: mcp_errors.ErrorCode.SERVER_ERROR,
}

def map_databricks_errors(func):
    """
    Decorator to catch databricks-sdk errors and map them to MCP standard errors.

    This decorator should be applied to MCP tool and resource methods that interact
    with the Databricks SDK. It translates specific SDK exceptions into
    exceptions that the MCP framework can understand and serialize correctly.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Execute the decorated function (tool or resource method)
            return func(*args, **kwargs)
        except Exception as e:
            original_error_message = str(e)
            error_data = {"original_error": original_error_message} # Optional data field

            # Default to a generic server error
            mcp_error_code = mcp_errors.ErrorCode.SERVER_ERROR
            log_as_warning = False

            # Check against our map of known Databricks errors
            for db_error_type, mapped_code in ERROR_MAP.items():
                if isinstance(e, db_error_type):
                    mcp_error_code = mapped_code
                    log_as_warning = True # Log mapped errors as warnings
                    log.warning(
                        "Mapped Databricks SDK error",
                        db_error=e.__class__.__name__,
                        mcp_code=mcp_error_code.value,
                        original_message=original_error_message,
                        tool_or_resource=func.__name__,
                    )
                    break # Use the first, most specific match

            if not log_as_warning:
                # If the error wasn't in our specific map, log it as an unexpected error
                log.error(
                    "Unhandled exception in tool/resource",
                    error=original_error_message,
                    tool_or_resource=func.__name__,
                    exc_info=True # Include full stack trace for unexpected errors
                )

            # Raise the appropriate MCPError exception.
            # The MCP framework (FastMCP) should catch this and format the JSON-RPC error response.
            # We pass the original message for clarity in the error response.
            raise mcp_errors.MCPError(
                code=mcp_error_code,
                message=f"Databricks Error: {original_error_message}",
                data=error_data
            )

    return wrapper
