import base64

import structlog
# Import the mcp instance from app.py
from ..app import mcp
# from mcp import errors as mcp_errors # Removed unused import

from ..config import settings  # Needed for conditional registration check
from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@mcp.tool(
    name="databricks-secrets-get_secret",
    description=(
        "Retrieves the value of a secret. WARNING: This exposes sensitive information. "
        "Use with extreme caution and ensure the server is appropriately secured."
    ),
)
def get_secret(scope_name: str, key: str) -> dict:
    """
    Retrieves the value of a secret.
    REQ-SEC-TOOL-01
    WARNING: Exposes sensitive data. Should be conditionally registered based on config.

    Args:
        scope_name: The name of the secret scope.
        key: The key name of the secret.
    """
    # Double-check config even if conditionally registered, as an extra layer
    if not settings.enable_get_secret:
        log.warning("Attempted to call get_secret tool but it is disabled by configuration.")
        # Raise a standard Python error. The error mapper decorator will handle conversion.
        raise PermissionError("Getting secret values is disabled by server configuration.")

    db = get_db_client()
    log.warning("Retrieving secret value (SECURITY SENSITIVE)", scope=scope_name, key=key) # Log sensitive operation clearly
    # The SDK's get_secret method returns the raw bytes value
    secret_value_bytes = db.secrets.get_secret(scope=scope_name, key=key).value
    # Attempt to decode as UTF-8, but return raw bytes if that fails
    try:
        secret_value_str = secret_value_bytes.decode('utf-8')
        log.info("Successfully retrieved secret (decoded as UTF-8)", scope=scope_name, key=key)
        return {"scope": scope_name, "key": key, "value_string": secret_value_str, "value_bytes": None}
    except UnicodeDecodeError:
        log.warning("Secret value is not valid UTF-8, returning raw bytes", scope=scope_name, key=key)
        # Return raw bytes as base64 for JSON compatibility?
        secret_value_b64 = base64.b64encode(secret_value_bytes).decode('ascii')
        return {"scope": scope_name, "key": key, "value_string": None, "value_base64": secret_value_b64}


@map_databricks_errors
@mcp.tool(
    name="databricks-secrets-put_secret",
    description="Creates or updates a secret with a string value.",
)
def put_secret(scope_name: str, key: str, secret_value: str) -> dict:
    """
    Creates or updates a secret with a string value.
    REQ-SEC-TOOL-02

    Args:
        scope_name: The name of the secret scope.
        key: The key name of the secret.
        secret_value: The string value to store in the secret.
    """
    db = get_db_client()
    log.info("Putting secret", scope=scope_name, key=key)
    db.secrets.put_secret(scope=scope_name, key=key, string_value=secret_value)
    status = "SUCCESS"
    log.info("Successfully put secret", scope=scope_name, key=key)
    return {"scope": scope_name, "key": key, "status": status}


# Consider adding put_secret_bytes if needed


@map_databricks_errors
@mcp.tool(
    name="databricks-secrets-delete_secret",
    description="Deletes a secret.",
)
def delete_secret(scope_name: str, key: str) -> dict:
    """
    Deletes a secret.
    REQ-SEC-TOOL-03

    Args:
        scope_name: The name of the secret scope.
        key: The key name of the secret to delete.
    """
    db = get_db_client()
    log.info("Deleting secret", scope=scope_name, key=key)
    db.secrets.delete_secret(scope=scope_name, key=key)
    status = "SUCCESS"
    log.info("Successfully deleted secret", scope=scope_name, key=key)
    return {"scope": scope_name, "key": key, "status": status}
