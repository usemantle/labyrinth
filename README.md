# labyrinth

Security graph discovery library — discovers code, databases, and cloud resources and stitches them into a queryable graph.

## Installation

Requires Python 3.13+.

```bash
git clone <repo-url> && cd labyrinth
uv sync
uv pip install -e .
```

The `labyrinth` command is now available inside the virtualenv:

```bash
.venv/bin/labyrinth --help
```

Or activate the virtualenv first:

```bash
source .venv/bin/activate
labyrinth --help
```

## Zsh Completions

**Option 1** — Eval in your `.zshrc` (simplest):

```bash
eval "$(_LABYRINTH_COMPLETE=zsh_source labyrinth)"
```

**Option 2** — Install the completion file to your fpath:

```bash
mkdir -p ~/.zfunc
cp completions/_labyrinth ~/.zfunc/_labyrinth
```

Then add to your `.zshrc` (before `compinit`):

```bash
fpath=(~/.zfunc $fpath)
autoload -Uz compinit && compinit
```

## Quick Start

```bash
labyrinth init my-project
```

This creates `~/.labyrinth/` and initializes a project directory under `~/.labyrinth/projects/my-project/`.
