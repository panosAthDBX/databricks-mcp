import logging
import sys

from mcp import FastMCP
from mcp.server.cli import run_stdio_server

from .config import load_config
from .auth import get_authenticated_client
from .databricks_client import DatabricksAPIClient

# Import resource and tool modules so FastMCP can discover the decorated functions
# These imports need to happen *before* mcp.run() or similar
from . import resources # Parent package import
from .resources import workspace as workspace_resources # Example explicit
from .resources import compute as compute_resources
from .resources import data as data_resources
from .resources import ml as ml_resources
from .resources import jobs as job_resources

from . import tools
from .tools import workspace as workspace_tools # Example explicit
from .tools import compute as compute_tools
from .tools import data as data_tools
from .tools import ml as ml_tools
from .tools import jobs as job_tools

# Import prompts if any (optional)
# from . import prompts
# from .prompts import code_review as code_review_prompts


# Configure basic logging
logging.basicConfig(
    level=logging.INFO, # Adjust level as needed (DEBUG, INFO, WARNING, ERROR)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr, # MCP often communicates via stdout, so log to stderr
)
logger = logging.getLogger("databricks_mcp.server")


# Initialize the MCP Server
# The name is used for identification by MCP clients
mcp_server = FastMCP(name="Databricks MCP Server")

# --- Global State / Context --- 
# It's often useful to have globally accessible state, like the API client.
# FastMCP allows adding context that can be accessed within tools/resources.

def setup_server_context():
    """Loads config, authenticates, and sets up global context."""
    try:
        config = load_config()
        workspace_client = get_authenticated_client(config)
        api_client = DatabricksAPIClient(workspace_client)
        
        # Add the api_client to the MCP server's context
        # It can be accessed in tools/resources via `mcp.context.api_client`
        mcp_server.context["api_client"] = api_client
        logger.info("Server context (including Databricks API client) initialized.")
        return True
    except (ValueError, ConnectionError) as e:
        logger.critical(f"Failed to initialize server context: {e}", exc_info=True)
        # Exit cleanly if core setup fails
        sys.exit(f"Error: Server initialization failed - {e}") 
    except Exception as e:
        logger.critical(f"Unexpected error during server context setup: {e}", exc_info=True)
        sys.exit(f"Error: Unexpected server initialization failure - {e}")

# --- Tool/Resource/Prompt Registration --- 
# FastMCP uses decorators (@mcp.tool, @mcp.resource, @mcp.prompt)
# By importing the modules where these decorators are used (as done above),
# FastMCP automatically discovers and registers them.

# --- Server Entry Point --- 
def main():
    """Main entry point to run the MCP server."""
    logger.info("Starting Databricks MCP Server setup...")
    
    # Setup global context before starting the server
    setup_server_context()
    
    logger.info("Databricks MCP Server ready. Running via stdio...")
    # Run the server using the standard stdio transport method
    # This is common for local integrations (like IDE plugins)
    # For HTTP/SSE, different setup would be needed.
    run_stdio_server(mcp_server)

if __name__ == "__main__":
    # This allows running the server directly using `python -m src.databricks_mcp`
    # or if this file itself is executed.
    main() 