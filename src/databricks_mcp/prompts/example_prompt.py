import structlog
from mcp import Prompt
from mcp import parameters

log = structlog.get_logger(__name__)

# Example Placeholder Prompt
@Prompt.from_callable(
    "databricks:prompts:example_analyze_data",
    description="Provides a template to guide an agent in analyzing data using available tools.",
    parameters=[
        Parameter(name="catalog", description="Catalog name", param_type=parameters.StringType),
        Parameter(name="schema", description="Schema name", param_type=parameters.StringType),
        Parameter(name="table", description="Table name", param_type=parameters.StringType),
        Parameter(name="analysis_goal", description="What specific insight is needed?", param_type=parameters.StringType),
    ],
    template=(
        "Analyze the table `{catalog}`.`{schema}`.`{table}` to achieve the following goal: {analysis_goal}.\n"
        "1. First, use `databricks:uc:get_table_schema` to understand the columns.\n"
        "2. Then, use `databricks:uc:preview_table` to see some sample data.\n"
        "3. Based on the schema and preview, formulate a SQL query using relevant columns to address the goal.\n"
        "4. Execute the query using `databricks:sql:execute_statement`.\n"
        "5. Retrieve the results using `databricks:sql:get_statement_result`.\n"
        "6. Summarize the findings based on the query results."
    )
)
def example_analyze_data_prompt(catalog: str, schema: str, table: str, analysis_goal: str):
    # This function body is not executed for prompts, only the definition matters.
    pass
