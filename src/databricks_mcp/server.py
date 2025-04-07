import structlog
from mcp.server.fastmcp import FastMCP

from . import resources  # Import the resources package

# Import error mapper (though not used directly here, tools/resources will use it)
# from .error_mapping import map_databricks_errors
# Import tool/resource modules here later when they are created
from . import tools  # Import the tools package
from .config import settings  # Import settings instance

# Import db_client to trigger initialization early (optional, but good for startup check)
from .db_client import get_db_client

# e.g., from . import prompts

log = structlog.get_logger(__name__)

# Initialize the FastMCP server instance
# The server_name should be unique if multiple MCP servers are used together.
mcp = FastMCP(server_name="databricks")

# --- Register Tools, Resources, Prompts ---
# Compute Capabilities
mcp.register(resources.compute.list_clusters)
mcp.register(resources.compute.get_cluster_details)
mcp.register(tools.compute.start_cluster)
mcp.register(tools.compute.terminate_cluster)

# Workspace & Repo Capabilities
mcp.register(resources.workspace.list_workspace_items)
mcp.register(resources.workspace.get_notebook_content)
mcp.register(resources.workspace.list_repos)
mcp.register(resources.workspace.get_repo_status)
mcp.register(tools.workspace.run_notebook)
mcp.register(tools.workspace.execute_code)

# Data & SQL Capabilities
mcp.register(resources.data.list_catalogs)
mcp.register(resources.data.list_schemas)
mcp.register(resources.data.list_tables)
mcp.register(resources.data.get_table_schema)
mcp.register(resources.data.preview_table)
mcp.register(resources.data.list_sql_warehouses)
mcp.register(tools.data.execute_sql)
mcp.register(tools.data.get_statement_result)
mcp.register(tools.data.start_sql_warehouse)
mcp.register(tools.data.stop_sql_warehouse)

# Job Capabilities
mcp.register(resources.jobs.list_jobs)
mcp.register(resources.jobs.get_job_details)
mcp.register(resources.jobs.list_job_runs)
mcp.register(tools.jobs.run_job_now)

# File Management Capabilities
mcp.register(resources.files.list_files)
mcp.register(tools.files.read_file)
mcp.register(tools.files.write_file)
mcp.register(tools.files.delete_file)
mcp.register(tools.files.create_directory)

# AI/ML Capabilities
mcp.register(resources.ml.list_mlflow_experiments)
mcp.register(resources.ml.list_mlflow_runs)
mcp.register(resources.ml.get_mlflow_run_details)
mcp.register(resources.ml.list_registered_models)
mcp.register(resources.ml.get_model_version_details)
mcp.register(tools.ml.query_model_serving_endpoint)
mcp.register(tools.ml.add_to_vector_index)
mcp.register(tools.ml.query_vector_index)

# Secrets Management Capabilities
mcp.register(resources.secrets.list_secret_scopes)
mcp.register(resources.secrets.list_secrets)
mcp.register(tools.secrets.put_secret)
mcp.register(tools.secrets.delete_secret)
# Conditionally register the sensitive get_secret tool
if settings.enable_get_secret:
    log.warning("Registering sensitive tool: databricks:secrets:get_secret")
    mcp.register(tools.secrets.get_secret)
else:
    log.info("Tool databricks:secrets:get_secret is disabled by configuration.")

# Example Prompts (Optional)
# from . import prompts
# mcp.register(prompts.example_prompt.example_analyze_data_prompt)

# -----------------------------------------

log.info("FastMCP server instance created", server_name=mcp.server_name)

# Perform an initial check of the Databricks client during server setup
try:
    get_db_client() # This will initialize if not already done and run the check
    log.info("Initial Databricks connection check successful.")
except Exception:
    # Error is already logged by get_db_client, just note failure
    log.critical("Databricks connection check FAILED during server setup. Server might not function correctly.")
    # Depending on desired behavior, we could exit here, but FastMCP might handle startup differently.
    # For now, allow it to continue starting but log critical failure.

# The 'mcp' instance will be imported and run by __main__.py
