# src/databricks_mcp/app.py
import structlog
from mcp.server.fastmcp import FastMCP

log = structlog.get_logger("app")

# Central definition of the MCP application instance
mcp = FastMCP(server_name="databricks")
log.debug("FastMCP instance created in app.py") 