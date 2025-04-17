import sys

import structlog

# Import and setup logging first
from .logging_config import setup_logging

setup_logging()

log = structlog.get_logger("main")

# Import server module first to ensure all decorators run and register capabilities
from . import server
# Import the central FastMCP instance from app.py
from .app import mcp


def main():
    log.info("Starting Databricks MCP Server...")
    try:
        # Run the FastMCP server (which now has registered capabilities)
        mcp.run()
        log.info("Databricks MCP Server finished.")
    except Exception as e:
        log.critical("Databricks MCP Server exited with an error", error=str(e), exc_info=True)
        exit(1) # Exit with error code if server fails catastrophically

if __name__ == "__main__":
    main()
