import structlog

# Import and setup logging first
from .logging_config import setup_logging

setup_logging()

log = structlog.get_logger(__name__)

# Import the configured FastMCP instance from server.py
from .server import mcp


def main():
    log.info("Starting Databricks MCP Server...")
    try:
        # Run the FastMCP server (this typically blocks, handling stdio)
        mcp.run()
        log.info("Databricks MCP Server finished.")
    except Exception as e:
        log.critical("Databricks MCP Server exited with an error", error=str(e), exc_info=True)
        exit(1) # Exit with error code if server fails catastrophically

if __name__ == "__main__":
    main()
