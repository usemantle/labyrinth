# CLI Development Directives

- Each CLI command must have a single function entrypoint decorated with `@cli.command()`.
- All CLI definitions use `rich_click` (imported as `import rich_click as click`).
- Helper/private functions are prefixed with `_` and live in `main.py` alongside the commands.
- When adding or modifying CLI commands, always update `completions/_labyrinth` to support the new commands.
- Use `tomllib` (stdlib) for reading TOML and `tomli_w` for writing TOML.
