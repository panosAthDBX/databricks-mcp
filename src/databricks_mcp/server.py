import structlog

from .config import settings
from .db_client import get_db_client
from .error_mapping import map_databricks_errors # Keep import if used by tools/resources

# Import all tool and resource modules so decorators attached to the app.mcp instance run
from .resources import compute as compute_resources
from .resources import data as data_resources
from .resources import files as files_resources
from .resources import jobs as jobs_resources
from .resources import ml as ml_resources
from .resources import secrets as secrets_resources
from .resources import workspace as workspace_resources

from .tools import compute as compute_tools
from .tools import data as data_tools
from .tools import files as files_tools
from .tools import jobs as jobs_tools
from .tools import ml as ml_tools
from .tools import secrets as secrets_tools
from .tools import workspace as workspace_tools

# Import prompts if any are defined and need discovery
# from . import prompts

log = structlog.get_logger(__name__)

# --- MCP Instance Definition Moved to app.py ---
# Remove: mcp = FastMCP(server_name="databricks")

# --- Conditional Logic Removed from Server --- 
# The runtime check remains within the tools.secrets.get_secret function.
if not settings.enable_get_secret:
    log.info("Tool databricks:secrets:get_secret is disabled by configuration (runtime check enforced).")
else:
    log.warning("Tool databricks:secrets:get_secret is enabled by configuration.")
# ----------------------------

log.info("Capability modules imported (discovery complete)")

# Perform an initial check of the Databricks client during server setup
try:
    get_db_client() # This will initialize if not already done and run the check
    log.info("Initial Databricks connection check successful.")
except Exception:
    log.critical("Databricks connection check FAILED during server setup. Server might not function correctly.")

# The actual mcp instance to run is imported from app.py in __main__.py
