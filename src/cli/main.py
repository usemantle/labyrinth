from __future__ import annotations

import tomllib
from pathlib import Path

import rich_click as click
import tomli_w
from iterfzf import iterfzf
from pydantic.fields import PydanticUndefined

from src.graph.credentials import CredentialBase, NoCredential
from src.graph.loaders import LOADER_REGISTRY
from src.graph.loaders.loader import ConceptLoader

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


def _get_active_project() -> str:
    """Read active project name from global config. Exits if none set."""
    config = _get_config()
    active = config.get("projects", {}).get("active")
    if not active:
        raise click.ClickException("No active project. Run 'labyrinth init <project>' first.")
    return active


def _get_project_config(project_name: str) -> dict:
    """Read per-project config from ~/.labyrinth/projects/{project}/config.toml."""
    path = PROJECTS_DIR / project_name / "config.toml"
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _save_project_config(project_name: str, config: dict) -> None:
    """Write per-project config to ~/.labyrinth/projects/{project}/config.toml."""
    path = PROJECTS_DIR / project_name / "config.toml"
    with path.open("wb") as f:
        tomli_w.dump(config, f)


def _select_loader() -> type[ConceptLoader]:
    """Present an fzf selector for available target types."""
    choices = {loader.display_name(): loader for loader in LOADER_REGISTRY}
    selection = iterfzf(choices.keys(), prompt="Select target type: ")
    if selection is None:
        raise click.ClickException("No target type selected.")
    return choices[selection]


def _prompt_urn_components(loader_cls: type[ConceptLoader]) -> dict[str, str]:
    """Prompt the user for each URN component declared by the loader."""
    components: dict[str, str] = {}
    for comp in loader_cls.urn_components():
        value = click.prompt(comp.description, default=comp.default)
        components[comp.name] = value
    return components


def _prompt_credentials(cred_type: type[CredentialBase]) -> dict:
    """Prompt for credential fields by introspecting the Pydantic model."""
    if cred_type is NoCredential:
        return {"type": "none"}

    result: dict = {"type": cred_type.model_fields["type"].default}
    for name, field_info in cred_type.model_fields.items():
        if name == "type":
            continue
        sensitive = name in ("password", "token", "secret")
        default = field_info.default if field_info.default is not PydanticUndefined else None
        result[name] = click.prompt(
            name.replace("_", " ").title(),
            default=default,
            hide_input=sensitive,
        )
    return result


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


@cli.command()
def add_target() -> None:
    """Register a target resource to the active project."""
    project = _get_active_project()

    loader_cls = _select_loader()

    components = _prompt_urn_components(loader_cls)
    urn = loader_cls.build_target_urn(**components)
    urn_str = str(urn)

    config = _get_project_config(project)
    config.setdefault("targets", [])

    for existing in config["targets"]:
        if existing.get("urn") == urn_str:
            raise click.ClickException(f"Target '{urn_str}' is already registered.")

    credentials = _prompt_credentials(loader_cls.credential_type())

    config["targets"].append({"urn": urn_str, "credentials": credentials})
    _save_project_config(project, config)

    click.echo(f"Added target '{urn_str}' to project '{project}'.")
