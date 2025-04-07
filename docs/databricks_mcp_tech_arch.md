# Technical Architecture: Databricks MCP Server

## 1. Overview

This document details the technical architecture for the Databricks MCP Server. This server implements the Model Context Protocol (MCP) to provide a standardized interface for AI agents (Clients/Hosts) to interact with the Databricks platform. It translates MCP requests into appropriate Databricks REST API calls using the official `databricks-sdk`.

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
        RequestRouter --> ErrorMapper[Error Mapper]

        AuthN --> DatabricksAPIWrapper(Databricks API Wrapper)
        ToolHandler --> DatabricksAPIWrapper
        ResourceHandler --> DatabricksAPIWrapper
        PromptHandler -.-> DatabricksAPIWrapper  // Prompts might indirectly use API info
        ErrorMapper -.-> DatabricksAPIWrapper

        DatabricksAPIWrapper --> Config(Server Configuration)
        AuthN --> Config
        DatabricksAPIWrapper --> Logger[Structured Logger]
        MCPInterface --> Logger
        RequestRouter --> Logger
    end

    subgraph Databricks Platform
        direction RL
        DatabricksAPI[Databricks REST APIs]
    end

    MCPClient <-.->|MCP (JSON-RPC over stdio/SSE)| MCPInterface
    DatabricksAPIWrapper <-->|HTTPS REST Calls via SDK| DatabricksAPI

    style HostApp fill:#f9f,stroke:#333,stroke-width:2px
    style MCPClient fill:#ccf,stroke:#333,stroke-width:2px
    style DatabricksMCPServer fill:#def,stroke:#333,stroke-width:2px
    style DatabricksPlatform fill:#fde,stroke:#333,stroke-width:2px
```

**Components:**

*   **MCP Host/Client:** The application initiating requests.
*   **Databricks MCP Server:**
    *   **MCP Interface:** Handles MCP communication (stdio, potentially HTTP/SSE).
    *   **Request Router:** Directs MCP requests to handlers.
    *   **Authentication Module:** Manages Databricks authentication using `databricks-sdk` mechanisms (PAT, OAuth, etc.) based on configuration.
    *   **Tool Handler:** Implements logic for MCP `Tool` requests.
    *   **Resource Handler:** Implements logic for MCP `Resource` requests.
    *   **Prompt Handler:** Manages and serves predefined MCP `Prompt` templates.
    *   **Error Mapper:** Translates `databricks-sdk` exceptions and API errors into standard MCP error responses.
    *   **Databricks API Wrapper:** Internal library using `databricks-sdk` to interact with Databricks APIs. Encapsulates SDK usage, pagination, and basic request logic.
    *   **Structured Logger:** Handles logging in a structured format (e.g., JSON) with correlation IDs and configurable levels.
    *   **Server Configuration:** Manages externalized configuration (env vars, config files) for Databricks host, auth, logging, rate limits, etc.
*   **Databricks Platform:** The target Databricks workspace.

## 3. Technology Stack & Project Structure

### 3.1 Technology Stack

*   **Language:** Python 3.9+
*   **MCP Framework:** `modelcontextprotocol/python-sdk` (likely installed as `pip install mcp` or `mcp-sdk`). Specifically using the `FastMCP` class (`from mcp.server.fastmcp import FastMCP`) for simplified server creation and capability registration via decorators (`@mcp.tool()`, `@mcp.resource()`, `@mcp.prompt()`).
*   **Databricks Interaction:** Official `databricks-sdk` (`pip install databricks-sdk`).
*   **Dependency Management:** `Poetry` (using `pyproject.toml` for managing dependencies, virtual environments, and packaging).
*   **Web Framework (for HTTP/SSE):** `FastAPI` integrates well if HTTP transport is needed, although `FastMCP` might handle some aspects internally.
*   **Configuration Management:** `pydantic-settings` for loading from environment variables/dotenv files, providing validation.
*   **Logging:** Standard `logging` module configured for structured output (e.g., using `structlog` or a custom JSON formatter).
*   **Testing:** `pytest`, `pytest-mock`, `pytest-asyncio` (if async), `databricks-sdk` mocking utilities if available, or custom mocks.
*   **Linting/Formatting:** `ruff` (configured via `pyproject.toml`).
*   **Deployment:** Docker container.

### 3.2 Recommended Project Structure

(Structure largely unchanged, updated main server file emphasis)

```
databricks-mcp-server/
├── .env.example
├── .gitignore
├── Dockerfile
├── pyproject.toml
├── README.md
├── src/
│   ├── databricks_mcp/
│   │   ├── __init__.py
│   │   ├── server.py        # Main FastMCP server initialization & capability registration
│   │   ├── config.py
│   │   ├── db_client.py
│   │   ├── error_mapping.py
│   │   ├── logging_config.py
│   │   ├── tools/           # Tool implementations (imported by server.py)
│   │   │   ├── __init__.py
│   │   │   └── ...
│   │   ├── resources/       # Resource implementations (imported by server.py)
│   │   │   ├── __init__.py
│   │   │   └── ...
│   │   └── prompts/         # Prompt definitions (imported by server.py)
│   │       ├── __init__.py
│   │       └── ...
│   └── __main__.py        # Entry point to run server.py
└── tests/
    └── ...
```

**Key Principles:**

*   **Modularity:** Group related Tools/Resources by Databricks area.
*   **Separation of Concerns:** `db_client.py` handles SDK interactions. Handlers use the client. `config.py` manages settings. `error_mapping.py` centralizes error translation. `logging_config.py` sets up logging.
*   **Discovery:** `FastMCP` discovers capabilities via decorators (`@mcp.tool`, etc.) when the modules containing them are imported in `server.py`.
*   **Packaging:** Standard Python packaging via `pyproject.toml`.

## 4. Design Considerations

### 4.1. MCP Implementation

*   **Protocol Compliance:** Adhere to JSON-RPC 2.0 and MCP specification using `FastMCP` from `modelcontextprotocol/python-sdk`.
*   **Transport Support:** `FastMCP` handles `stdio` transport directly. HTTP/SSE support might require integration with FastAPI or using built-in `FastMCP` options if available.
*   **Capabilities Definition:** Use the decorators (`@mcp.tool()`, `@mcp.resource()`, `@mcp.prompt()`) provided by the SDK on functions within the `tools/`, `resources/`, and `prompts/` modules. Ensure clear docstrings and type hints for parameters, as these are used for discovery and LLM interaction.

### 4.2. Databricks API Interaction

*   **SDK Usage:** Utilize `databricks-sdk` exclusively for all API interactions within `db_client.py`.
*   **Error Handling:** Implement the `error_mapping.py` module as described previously, mapping `databricks-sdk` exceptions to MCP errors.
*   **Asynchronous Operations:** Follow the recommended approach (Option 1: return ID, provide status check tool) for long-running operations, documenting clearly in tool descriptions.
*   **Pagination:** Leverage `databricks-sdk` automatic pagination where possible. Implement manual handling in `db_client.py` if needed.

### 4.3. Authentication and Security

*   **Credential Management:** Rely on `databricks-sdk` credential providers. Configure via environment variables / `config.py`.
*   **Permissions:** Delegate to Databricks. Map permission errors correctly.
*   **Input Validation:** Use Pydantic for basic validation via type hints in decorated functions. Add specific checks within function bodies if required.
*   **Secrets Tool (`get_secret`):** Make registration conditional via `ENABLE_GET_SECRET` flag in `server.py`.
*   **Rate Limiting:** Implement if using HTTP transport (e.g., via FastAPI middleware).

### 4.4. Configuration

*   Use `pydantic-settings` in `config.py` as described.
*   Load from environment variables / `.env`.
*   Inject configuration where needed.

### 4.5. Logging (NFR-REL-02)

*   Implement structured logging (JSON) via `logging_config.py`.
*   Configure level via `LOG_LEVEL`.
*   Include correlation IDs.
*   Mask sensitive data.

### 4.6. Testing (NFR-MAINT-01)

*   Mock `db_client.py` for unit tests.
*   Focus on testing handlers, error mapping, configuration.
*   Consider integration tests carefully against a non-production environment or using recorded API interactions.
*   Aim for high unit test coverage.

## 5. Deployment

*   **Containerization:** `Dockerfile` using standard Python image, installing dependencies, copying `src`.
*   **Entry Point:** `CMD ["python", "-m", "src.databricks_mcp"]` (assuming `__main__.py` imports and runs the `FastMCP` instance from `server.py`).
*   **Configuration:** Via environment variables.
*   **Transport:** Default `stdio`. Expose port if HTTP is configured.
*   **Process Management:** Standard tools (Kubernetes, systemd, etc.).

## 6. Future Enhancements

*   Implement OAuth 2.0 login flow if required (more complex than PAT/basic auth).
*   Add support for Databricks configuration profiles (`~/.databrickscfg`).
*   Implement optional response caching for read-only, static resources.
*   Support creating/updating Databricks resources (Jobs, Clusters etc.) via dedicated tools. 