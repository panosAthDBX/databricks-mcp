# Technical Architecture: Databricks MCP Server

## 1. Overview

This document details the technical architecture for the Databricks MCP Server. This server implements the Model Context Protocol (MCP) to provide a standardized interface for AI agents (Clients/Hosts) to interact with the Databricks platform. It translates MCP requests into appropriate Databricks REST API calls.

## 2. Architecture Diagram

```mermaid
graph LR
    subgraph AI Agent / MCP Host
        direction LR
        HostApp[Host Application (e.g., IDE, Chatbot)] --> MCPClient(MCP Client)
    end

    subgraph Databricks MCP Server
        direction TB
        MCPInterface[MCP Interface (stdio/HTTP+SSE)] --> RequestRouter{Request Router}
        RequestRouter --> AuthN[Authentication Module]
        RequestRouter --> ToolHandler[Tool Handler]
        RequestRouter --> ResourceHandler[Resource Handler]
        RequestRouter --> PromptHandler[Prompt Handler]

        AuthN --> DatabricksAPIWrapper(Databricks API Wrapper)
        ToolHandler --> DatabricksAPIWrapper
        ResourceHandler --> DatabricksAPIWrapper
        PromptHandler -.-> DatabricksAPIWrapper  // Prompts might indirectly use API info

        DatabricksAPIWrapper --> Config(Server Configuration)
        AuthN --> Config
    end

    subgraph Databricks Platform
        direction RL
        DatabricksAPI[Databricks REST APIs]
    end

    MCPClient <-.->|MCP (JSON-RPC over stdio/SSE)| MCPInterface
    DatabricksAPIWrapper <-->|HTTPS REST Calls| DatabricksAPI

    style HostApp fill:#f9f,stroke:#333,stroke-width:2px
    style MCPClient fill:#ccf,stroke:#333,stroke-width:2px
    style DatabricksMCPServer fill:#def,stroke:#333,stroke-width:2px
    style DatabricksPlatform fill:#fde,stroke:#333,stroke-width:2px
```

**Components:**

*   **MCP Host/Client:** The application (e.g., AI assistant, IDE plugin) that initiates requests to the MCP server.
*   **Databricks MCP Server:** The core application described in this document.
    *   **MCP Interface:** Handles communication with MCP Clients via supported transports (stdio, HTTP/SSE), parsing incoming JSON-RPC requests and formatting outgoing responses.
    *   **Request Router:** Directs incoming MCP requests (discovery, invocation) to the appropriate handler based on the request type and method.
    *   **Authentication Module:** Manages authentication with Databricks using configured credentials (PAT, OAuth, etc.). Obtains and refreshes tokens as necessary.
    *   **Tool Handler:** Implements the logic for executing MCP `Tool` requests (e.g., `execute_sql`, `run_notebook`). Maps MCP tool parameters to Databricks API calls.
    *   **Resource Handler:** Implements the logic for serving MCP `Resource` requests (e.g., `list_clusters`, `get_table_schema`). Maps MCP resource identifiers to Databricks API calls.
    *   **Prompt Handler:** Manages and serves predefined MCP `Prompt` templates.
    *   **Databricks API Wrapper:** An internal library abstracting the specifics of calling Databricks REST APIs. Handles endpoint construction, request formatting, response parsing, and error handling.
    *   **Server Configuration:** Stores settings like Databricks host URL, authentication credentials (securely), logging levels, etc.
*   **Databricks Platform:** The target Databricks workspace exposing its functionalities via REST APIs.

## 3. Technology Stack & Project Structure

This section details the recommended technologies and a potential project layout following best practices for Python MCP server development.

### 3.1 Technology Stack

*   **Language:** Python 3.9+
*   **MCP Framework:** `modelcontextprotocol/python-sdk` (specifically using its `FastMCP` implementation). This library provides decorators (`@mcp.tool()`, `@mcp.resource()`, `@mcp.prompt()`) simplifying the implementation of MCP capabilities and handling protocol details.
*   **Databricks Interaction:** Official `databricks-sdk` for Python. This SDK simplifies authentication and interaction with the Databricks REST APIs.
*   **Dependency Management:** `uv` (recommended by `modelcontextprotocol/python-sdk`) or `pip` with `pip-tools` for managing dependencies via `pyproject.toml` or `requirements.in` / `requirements.txt`.
*   **Web Framework (Optional):** If implementing the HTTP/SSE transport, `FastAPI` is recommended for its performance and async capabilities. `FastMCP` might handle the server setup internally depending on usage.
*   **Configuration Management:** Environment variables (loaded via `python-dotenv` during development) are preferred for containerized deployments. TOML (via `pyproject.toml` or a separate file) or YAML are alternatives for more complex configurations.
*   **Testing:** `pytest` framework with `pytest-mock` for unit tests and potentially `pytest-asyncio` if using async code.
*   **Linting/Formatting:** `ruff` (for linting and formatting) or a combination of `flake8`/`pylint` and `black`/`isort`.
*   **Deployment:** Docker container is the recommended artifact for deployment.

### 3.2 Recommended Project Structure

A modular structure is recommended for maintainability and scalability:

```
databricks-mcp-server/
├── .env.example           # Example environment variables
├── .gitignore
├── Dockerfile             # For containerizing the server
├── pyproject.toml         # Project metadata and dependencies (or requirements.txt)
├── README.md
├── src/
│   ├── databricks_mcp/
│   │   ├── __init__.py
│   │   ├── server.py        # Main FastMCP server initialization and entry point
│   │   ├── config.py        # Configuration loading logic
│   │   ├── databricks_client.py # Wrapper/client for databricks-sdk interactions
│   │   ├── auth.py          # Authentication logic/setup for databricks_client
│   │   ├── tools/           # Directory for MCP Tool implementations
│   │   │   ├── __init__.py
│   │   │   ├── workspace.py # Workspace-related tools (e.g., run_notebook)
│   │   │   ├── compute.py   # Compute-related tools (e.g., start_cluster)
│   │   │   └── ...          # Other tool categories (data, ml, jobs)
│   │   ├── resources/       # Directory for MCP Resource implementations
│   │   │   ├── __init__.py
│   │   │   ├── workspace.py # Workspace-related resources (e.g., list_workspace_items)
│   │   │   ├── compute.py   # Compute-related resources (e.g., list_clusters)
│   │   │   └── ...          # Other resource categories (data, ml, jobs)
│   │   └── prompts/         # Optional: Directory for MCP Prompt definitions
│   │       ├── __init__.py
│   │       └── code_review.py # Example prompt
│   └── __main__.py        # Allows running the server module directly (python -m src.databricks_mcp)
└── tests/                 # Unit and integration tests
    ├── __init__.py
    ├── fixtures/          # Test fixtures (e.g., mocked API responses)
    ├── unit/              # Unit tests for handlers, client wrapper, etc.
    │   ├── __init__.py
    │   └── ...
    └── integration/       # Integration tests (require Databricks connection or mock server)
        ├── __init__.py
        └── ...

```

**Key Principles:**

*   **Modularity:** Group related Tools and Resources into separate files within `src/databricks_mcp/tools/` and `src/databricks_mcp/resources/`.
*   **Separation of Concerns:** The `databricks_client.py` encapsulates all direct interaction logic with the `databricks-sdk`. Handlers in `tools/` and `resources/` use this client. `config.py` handles configuration loading. `auth.py` manages authentication setup.
*   **Discovery:** The main `server.py` imports the tool/resource modules, allowing `FastMCP` to discover the decorated functions.
*   **Packaging:** Use standard Python packaging (`pyproject.toml` or `setup.py`/`setup.cfg`) for installable distribution if needed.

## 4. Design Considerations

### 4.1. MCP Implementation

*   **Protocol Compliance:** Strictly adhere to the MCP specification for discovery responses, tool/resource/prompt definitions, and request/response formats (JSON-RPC 2.0).
*   **Transport Support:** Initially support `stdio` for local use cases. Implement HTTP/SSE transport for broader network accessibility.
*   **Capabilities Definition:** Tools, Resources, and Prompts will be defined within the Python code using decorators provided by the chosen MCP library (e.g., `@mcp.tool()`, `@mcp.resource()`). Descriptions should be clear and informative for discovery by clients.

### 4.2. Databricks API Interaction

*   **SDK Usage:** Leverage the official `databricks-sdk` for Python to simplify API interactions, authentication, and object handling.
*   **Error Handling:** Implement robust error handling for Databricks API calls. Translate API errors (e.g., 4xx, 5xx HTTP codes, specific Databricks error codes) into appropriate MCP error responses.
*   **Asynchronous Operations:** For potentially long-running Databricks operations initiated by Tools (e.g., `run_notebook`, `run_job_now`), the MCP server should ideally return immediately with an identifier, and potentially provide a separate Tool or mechanism to check the status later. However, the initial implementation might block until completion for simplicity, clearly documenting this behavior.
*   **Pagination:** Handle paginated responses from Databricks APIs correctly when implementing Resource handlers (e.g., `list_tables`, `list_job_runs`).

### 4.3. Authentication and Security

*   **Credential Storage:** Databricks credentials (e.g., PAT) MUST NOT be hardcoded. Use secure methods like environment variables, secrets management systems, or secure configuration files with restricted permissions.
*   **Scope:** The server operates under the permissions of the configured Databricks principal. No internal permission mapping will be performed; access control is delegated entirely to Databricks.
*   **Input Sanitization:** While MCP itself is generally trusted, basic validation of parameters received in MCP requests (e.g., types, expected formats) is advisable before passing them to the Databricks API wrapper.

### 4.4. Configuration

*   **Required:** Databricks Host URL, Authentication Method (e.g., 'pat', 'oauth'), Credentials.
*   **Optional:** Logging level, specific transport configurations (e.g., HTTP port).
*   Use environment variables as the primary configuration mechanism, suitable for containerized deployments.

### 4.5. Testing

*   **Unit Tests:** Test individual modules (handlers, API wrapper logic) using mocking for Databricks API calls and MCP framework interactions.
*   **Integration Tests:** Test the end-to-end flow from receiving an MCP request to interacting with a *test* Databricks workspace (requires careful setup and teardown, or using mocked API endpoints).

## 5. Deployment

*   **Containerization:** Package the server as a Docker image for easy deployment and dependency management.
*   **Execution:** Can be run as a standalone process (interacting via `stdio`) or as a network service (if HTTP/SSE is implemented).
*   **Process Management:** Use a process manager (like `systemd` or a container orchestrator like Kubernetes) for production deployments to ensure reliability and restarts.

## 6. Future Enhancements

*   **OAuth Support:** Implement OAuth 2.0 flows for more secure and user-friendly authentication compared to PATs.
*   **Configuration Profiles:** Allow selection of different Databricks environments (dev/staging/prod) via configuration.
*   **Caching:** Implement optional caching for frequently accessed, slow-changing resources (e.g., cluster lists) with appropriate TTLs. 