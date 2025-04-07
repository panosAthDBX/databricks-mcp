import functools
import structlog
from databricks.sdk.errors import NotFound, PermissionDenied, BadRequest, DatabricksError, ResourceDoesNotExist
# Remove mcp errors import

log = structlog.get_logger(__name__)

# Standard JSON-RPC Error Codes + Reserved Server Range
# https://www.jsonrpc.org/specification#error_object
CODE_INVALID_PARAMS = -32602
CODE_INTERNAL_ERROR = -32603 # Fallback for unexpected server errors
# Implementation-defined server errors (-32000 to -32099)
CODE_SERVER_ERROR = -32000
CODE_RESOURCE_NOT_FOUND = -32001
CODE_PERMISSION_DENIED = -32003
CODE_RATE_LIMIT = -32005 # Arbitrary choice in server range


# Map specific Databricks exceptions to MCP error codes.
ERROR_MAP = {
    NotFound: CODE_RESOURCE_NOT_FOUND,
    ResourceDoesNotExist: CODE_RESOURCE_NOT_FOUND,
    PermissionDenied: CODE_PERMISSION_DENIED,
    BadRequest: CODE_INVALID_PARAMS, # Often indicates bad input from client
}

# Define common error codes (check SDK source or docs for exact codes if possible)
RATE_LIMIT_ERROR_CODE_STR = "REQUEST_LIMIT_EXCEEDED" # Databricks SDK error_code string

def map_databricks_errors(func):
    """
    Decorator to catch databricks-sdk errors and map them to standard errors.

    Raises standard Exceptions with a message. The MCP framework should
    convert these into JSON-RPC error responses.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            original_error_message = str(e)
            # Remove error_data as Exception doesn't take it directly
            # error_data = {"original_error": original_error_message}

            # Default error code
            mcp_error_code = CODE_SERVER_ERROR
            log_as_warning = False
            is_mapped = False

            # Check specific mapped exceptions first
            for db_error_type, mapped_code in ERROR_MAP.items():
                if isinstance(e, db_error_type):
                    mcp_error_code = mapped_code
                    log_as_warning = True # Log known mappings as warnings
                    is_mapped = True
                    log.warning(
                        "Mapped specific Databricks SDK error",
                        db_error=e.__class__.__name__,
                        mcp_code=mcp_error_code,
                        original_message=original_error_message,
                        tool_or_resource=func.__name__,
                    )
                    break

            # Handle general DatabricksError (check for rate limit)
            if not is_mapped and isinstance(e, DatabricksError):
                is_mapped = True
                if getattr(e, 'error_code', None) == RATE_LIMIT_ERROR_CODE_STR:
                    mcp_error_code = CODE_RATE_LIMIT
                    log_as_warning = True
                    log.warning(
                        "Mapped Databricks Rate Limit error",
                        db_error=e.__class__.__name__,
                        error_code=RATE_LIMIT_ERROR_CODE_STR,
                        mcp_code=mcp_error_code,
                        original_message=original_error_message,
                        tool_or_resource=func.__name__,
                    )
                else:
                    # Unmapped DatabricksError -> Generic Server Error
                    mcp_error_code = CODE_SERVER_ERROR
                    log_as_warning = False
                    log.error(
                        "Unhandled Databricks SDK error",
                        db_error=e.__class__.__name__,
                        error_code=getattr(e, 'error_code', 'UNKNOWN'),
                        mcp_code=mcp_error_code,
                        original_message=original_error_message,
                        tool_or_resource=func.__name__,
                        exc_info=True
                    )

            # Log unhandled non-Databricks errors
            if not is_mapped:
                mcp_error_code = CODE_INTERNAL_ERROR # Use JSON-RPC internal error code
                log.error(
                    "Unhandled non-Databricks exception in tool/resource",
                    error_type=e.__class__.__name__,
                    error=original_error_message,
                    tool_or_resource=func.__name__,
                    exc_info=True
                )

            # Raise a standard Exception. The MCP framework should catch this.
            # Prepending the code might help framework map it, or it might use the message.
            # Let's just provide a clear message.
            raise Exception(f"[MCP Error Code {mcp_error_code}] Databricks Error ({e.__class__.__name__}): {original_error_message}")

    return wrapper

