### Project Summary
This project intends to be a knowledge base for your AI agent to ingest:
* Your codebase
* Infrastructure configuration
* Database configuration
* Identity configuration
* And more

### Project Structure
```
├── src
│   ├── cli         # All cli code, no other directories should import from here
│   ├── drivers     # Abstractions to allow for communicating with external services (i.e. databases)
│   ├── graph       # The core of the knowledge graph, ingestors, data models, and plugins for specific languages/frameworks
│   └── mcp         # An MCP server that exposes the graph in an AI friendly way
```

### Code Style Rules
* All imports must be at module level. Never import inside a function unless it is strictly necessary to avoid a circular dependency.
* Run `uv run ruff check` on all changed files before committing. Fix all errors.