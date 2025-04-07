import structlog
from mcp import Parameter
from mcp import Resource
from mcp import parameters

from ..db_client import get_db_client
from ..error_mapping import map_databricks_errors

log = structlog.get_logger(__name__)

@map_databricks_errors
@Resource.from_callable(
    "databricks:secrets:list_scopes",
    description="Lists available secret scopes.",
    parameters=[]
)
def list_secret_scopes() -> list[dict]:
    """
    Lists available secret scopes.
    REQ-SEC-RES-01
    """
    db = get_db_client()
    log.info("Listing secret scopes")
    scopes = db.secrets.list_scopes()
    result = [
        {
            "name": scope.name,
            # Add backend type if useful, e.g., scope.backend_type
        }
        for scope in scopes.scopes # API returns list inside object
        if scope.name is not None
    ]
    log.info("Successfully listed secret scopes", count=len(result))
    return result


@map_databricks_errors
@Resource.from_callable(
    "databricks:secrets:list_secrets",
    description="Lists secret keys within a specified scope (values are NOT returned).",
     parameters=[
        Parameter(
            name="scope_name",
            description="The name of the secret scope.",
            param_type=parameters.StringType,
            required=True,
        )
    ]
)
def list_secrets(scope_name: str) -> list[dict]:
    """
    Lists secret keys within a scope (does *not* return values).
    REQ-SEC-RES-02
    """
    db = get_db_client()
    log.info("Listing secret keys in scope", scope_name=scope_name)
    secrets_list = db.secrets.list_secrets(scope=scope_name)
    result = [
        {
            "key": secret.key,
            "last_updated_timestamp": secret.last_updated_timestamp, # Timestamp ms
        }
        for secret in secrets_list.secrets # API returns list inside object
        if secret.key is not None
    ]
    log.info("Successfully listed secret keys", scope_name=scope_name, count=len(result))
    return result
