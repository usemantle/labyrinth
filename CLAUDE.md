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

### CLI Commands
 I would like you to propose 10 ideas to improve the current functionality of this MCP plugin that would make you a more effective security threat     
  hunter. Some ideas I have are:                                                                                                                         
  * Integrate with live systems such as AWS, Azure, GCP, Cloudflare, Vercel, Supabase, etc. Constantly reindex these systems and link it back to         
  offline systems.                                                                                                                                       
  * Add in the concept of identity:                                                                                                                      
    * database users                                                                                                                                     
    * IAM roles                                                                                                                                          
    * IdP linked users                                                                                                                                   
    * OIDC token issuers and users                                                                                                                       
    * etc                                                                                                                                                
  * Add a remediation functionality for dependencies  