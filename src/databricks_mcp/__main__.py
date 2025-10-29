import sys
import traceback


def _get_logger():
    try:
        from .logging_config import setup_logging
        import structlog  # noqa: F401

        setup_logging()
        return __import__("structlog").get_logger("main")
    except ModuleNotFoundError:
        import logging

        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        return logging.getLogger("main")


def main():
    log = _get_logger()
    log.info("Starting Databricks MCP Server...")

    try:
        # Import server first so capability registration decorators run
        from . import server  # noqa: F401
        # Import the central FastMCP instance
        from .app import mcp
    except ModuleNotFoundError as e:
        msg = (
            "Missing dependency detected: "
            f"{e.name}. Run 'poetry install' to install project dependencies."
        )
        try:
            log.critical(msg)
        except Exception:
            print(msg, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        try:
            log.critical("Error during server initialization", error=str(e), exc_info=True)
        except Exception:
            traceback.print_exc()
        sys.exit(1)

    try:
        mcp.run()
        log.info("Databricks MCP Server finished.")
    except Exception as e:
        try:
            log.critical("Databricks MCP Server exited with an error", error=str(e), exc_info=True)
        except Exception:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
