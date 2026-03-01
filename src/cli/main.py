from __future__ import annotations

import tomllib
from pathlib import Path

import rich_click as click
import tomli_w

LABYRINTH_DIR = Path.home() / ".labyrinth"
PROJECTS_DIR = LABYRINTH_DIR / "projects"
CONFIG_PATH = LABYRINTH_DIR / "config.toml"


def _maybe_create_labyrinth_dir() -> Path:
    """Ensure ~/.labyrinth and ~/.labyrinth/projects exist."""
    LABYRINTH_DIR.mkdir(exist_ok=True)
    PROJECTS_DIR.mkdir(exist_ok=True)
    return LABYRINTH_DIR


def _get_config() -> dict:
    """Read ~/.labyrinth/config.toml, returning empty dict if absent."""
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


def _save_config(config: dict) -> None:
    """Write config dict to ~/.labyrinth/config.toml."""
    with CONFIG_PATH.open("wb") as f:
        tomli_w.dump(config, f)


@click.group()
@click.version_option(package_name="labyrinth")
def cli() -> None:
    """Labyrinth -- security graph discovery CLI."""


@cli.command()
@click.argument("project_name")
def init(project_name: str) -> None:
    """Initialize a new labyrinth project."""
    _maybe_create_labyrinth_dir()

    project_path = PROJECTS_DIR / project_name
    if project_path.exists():
        click.echo(f"Project '{project_name}' already exists.")
    else:
        project_path.mkdir()
        click.echo(f"Created project '{project_name}'.")

    config = _get_config()
    config.setdefault("projects", {})
    config["projects"]["active"] = project_name
    _save_config(config)

    click.echo(f"Active project set to '{project_name}'.")
