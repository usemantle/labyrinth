# Labyrinth

![Coverage](./coverage.svg)

Labyrinth is an AI knowledge base that discovers code, databases, and cloud resources and stitches them into a queryable graph for your AI agent.

## Quick Start
Currently we only support installing and running from source:

## Install
Requires Python 3.13+.

```bash
git clone <repo-url> && cd labyrinth
uv sync
uv pip install -e .
```

## Getting Started

### 1. Create a project

```bash
labyrinth init my-project
```

This creates `~/.labyrinth/` and initializes a project directory under `~/.labyrinth/projects/my-project/`. The project is automatically set as the active project.

### 2. Add targets

Register targets for your project:

```bash
labyrinth add-target
```

An interactive fuzzy selector presents the available target types (more coming soon):

| Target | URN components | Credentials |
|--------|---------------|-------------|
| **PostgreSQL** | host, port, database | username / password |
| **AWS S3 Bucket** | account ID, region, bucket | AWS profile |
| **Local Codebase** | path to directory | none |
| **GitHub Repository** | org, repo | none |

After selecting a type you'll be prompted for the URN components and any required credentials. Repeat `add-target` for each target you want in the graph.

### 3. Add plugins (codebase targets)

Codebase targets support plugins that enrich the graph with framework-specific metadata:

```bash
labyrinth add-plugin
```

Select a codebase target, then pick a plugin:

| Plugin | What it detects |
|--------|----------------|
| **sqlalchemy** | ORM model classes with `__tablename__`, tags nodes with the mapped table name |
| **fastapi** | Route decorators (`@router.get`, etc.), resolves full route paths including `APIRouter` and `include_router` prefixes |
| **boto3-s3** | S3 client creation and API calls (`put_object`, `get_object`, etc.), tags with operation types |

Plugins run automatically during scan. You can add multiple plugins to the same target.

### 4. Scan

Build the security graph from all registered targets:

```bash
labyrinth scan
```

You'll be prompted to scan a specific target or all targets at once. The scan:

1. Connects to each datasource and extracts nodes (databases, tables, columns, files, classes, functions, etc.)
2. Runs language-specific analysis (Python import/call resolution)
3. Runs plugins for framework-specific enrichment
4. Stitches cross-domain edges (e.g. `CODE_TO_DATA` linking ORM models to database tables)
5. Writes the result to `~/.labyrinth/projects/<project>/graph.json`

### 5. Visualize

Launch an interactive graph visualization in the browser:

```bash
labyrinth visualize
```

This starts a local server and opens the graph viewer with:

- Node filtering by type (database, table, file, class, function, etc.)
- Edge filtering by relationship (CONTAINS, CODE_TO_DATA, CODE_TO_CODE)
- ForceAtlas2, circular, and random layouts
- Hover tooltips showing node metadata
- A stats bar with node/edge counts

Use `--port` to change the default port (8787):

```bash
labyrinth visualize --port 9000
```

### Managing your project

```bash
labyrinth describe-project   # Print the active project's configuration
labyrinth remove-target      # Interactively remove a target
```


## Zsh Completions

Eval in your `.zshrc` (simplest):

```bash
eval "$(_LABYRINTH_COMPLETE=zsh_source labyrinth)"
```