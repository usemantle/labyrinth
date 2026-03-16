from __future__ import annotations

import logging
import tomllib
import uuid
from pathlib import Path

import rich_click as click
import tomli_w
from iterfzf import iterfzf
from pydantic.fields import PydanticUndefined

from src.cli.settings import SETTING_DEFINITIONS, SETTING_GROUPS, get_setting, set_setting
from src.graph.credentials import CredentialBase, NoCredential
from src.graph.loaders import LOADER_REGISTRY
from src.graph.loaders.loader import ConceptLoader

logger = logging.getLogger(__name__)

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
        project_config = _get_project_config(project_name)
        project_config["project_id"] = str(uuid.uuid4())
        _save_project_config(project_name, project_config)
        click.echo(f"Created project '{project_name}'.")

    config = _get_config()
    config.setdefault("projects", {})
    config["projects"]["active"] = project_name
    _save_config(config)

    click.echo(f"Active project set to '{project_name}'.")


@cli.command("set-active")
def set_active() -> None:
    """Switch the active project."""
    _maybe_create_labyrinth_dir()

    projects = sorted(p.name for p in PROJECTS_DIR.iterdir() if p.is_dir())
    if not projects:
        raise click.ClickException("No projects found. Run 'labyrinth init <project>' first.")

    selected = iterfzf(projects, prompt="Select project: ")
    if selected is None:
        raise click.ClickException("No project selected.")

    config = _get_config()
    config.setdefault("projects", {})
    config["projects"]["active"] = selected
    _save_config(config)

    click.echo(f"Active project set to '{selected}'.")


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

@cli.command("describe")
def describe() -> None:
    """Pretty-print the active project's configuration."""
    project = _get_active_project()
    config = _get_project_config(project)
    if not config:
        raise click.ClickException(f"Project '{project}' has no configuration.")
    click.echo(tomli_w.dumps(config))


@cli.command()
def remove_target() -> None:
    """Remove a target from the active project."""
    project = _get_active_project()
    config = _get_project_config(project)
    targets = config.get("targets", [])
    if not targets:
        raise click.ClickException("No targets registered.")

    urn_labels = [t["urn"] for t in targets]
    selected = iterfzf(urn_labels, prompt="Select target to remove: ")
    if selected is None:
        raise click.ClickException("No target selected.")

    config["targets"] = [t for t in targets if t["urn"] != selected]
    _save_project_config(project, config)
    click.echo(f"Removed target '{selected}' from project '{project}'.")


@cli.command()
def add_plugin() -> None:
    """Add a plugin to a codebase target in the active project."""
    from src.graph.scanner import _resolve_loader

    project = _get_active_project()
    config = _get_project_config(project)
    targets = config.get("targets", [])
    if not targets:
        raise click.ClickException("No targets registered. Run 'labyrinth add-target' first.")

    urn_labels = [t["urn"] for t in targets]
    selected_urn = iterfzf(urn_labels, prompt="Select target: ")
    if selected_urn is None:
        raise click.ClickException("No target selected.")

    from src.graph.graph_models import URN
    urn = URN(selected_urn)
    loader_cls = _resolve_loader(urn)

    available = loader_cls.available_plugins()
    if not available:
        raise click.ClickException(
            f"Target '{selected_urn}' does not support plugins "
            f"({loader_cls.display_name()} has no available plugins)."
        )

    target = next(t for t in targets if t["urn"] == selected_urn)
    existing = target.get("plugins", [])
    remaining = {k: v for k, v in available.items() if k not in existing}
    if not remaining:
        raise click.ClickException("All available plugins are already added to this target.")

    selected_plugin = iterfzf(remaining.keys(), prompt="Select plugin: ")
    if selected_plugin is None:
        raise click.ClickException("No plugin selected.")

    target.setdefault("plugins", []).append(selected_plugin)
    _save_project_config(project, config)
    click.echo(f"Added plugin '{selected_plugin}' to target '{selected_urn}'.")


@cli.command()
def scan() -> None:
    """Scan registered targets and build the security graph."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    from src.graph.scanner import Scanner
    from src.graph.sinks.json_file_sink import JsonFileSink

    project = _get_active_project()
    config = _get_project_config(project)

    project_id_str = config.get("project_id")
    if not project_id_str:
        raise click.ClickException(
            f"Project '{project}' has no project_id. "
            "Re-run 'labyrinth init' to generate one."
        )
    project_id = uuid.UUID(project_id_str)

    targets = config.get("targets", [])
    if not targets:
        raise click.ClickException(
            "No targets registered. Run 'labyrinth add-target' first."
        )

    # Let user pick a specific target or scan all.
    urn_labels = [t["urn"] for t in targets]
    choices = ["[All targets]"] + urn_labels
    selected = iterfzf(choices, prompt="Select target to scan: ")
    if selected is None:
        raise click.ClickException("No target selected.")
    if selected != "[All targets]":
        targets = [t for t in targets if t["urn"] == selected]

    project_dir = PROJECTS_DIR / project
    output_path = project_dir / "graph.json"
    sink = JsonFileSink(output_path)

    global_config = _get_config()
    Scanner(project, project_id, targets, sink, project_dir, global_config).run()
    click.echo(f"Graph written to {output_path}")


@cli.command()
@click.option(
    "--graph-path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Explicit path to graph.json. Overrides the active project.",
)
def mcp(graph_path: Path | None) -> None:
    """Start the MCP server for the active project's knowledge graph."""
    import sys

    # Redirect logging to stderr — stdout is reserved for MCP protocol.
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    if graph_path is None:
        project = _get_active_project()
        project_dir = PROJECTS_DIR / project
        graph_path = project_dir / "graph.json"

    if not graph_path.exists():
        raise click.ClickException(
            f"No graph.json found at '{graph_path}'. Run 'labyrinth scan' first."
        )

    from src.mcp.main import run_mcp_server

    run_mcp_server(graph_path)


@cli.command()
@click.option("--port", default=8787, show_default=True, help="Port for the local HTTP server.")
def serve(port: int) -> None:
    """Launch the Labyrinth dashboard: graph visualization and agent actions."""
    import json
    import shutil
    import webbrowser
    from functools import partial
    from http.server import HTTPServer, SimpleHTTPRequestHandler

    project = _get_active_project()
    project_dir = PROJECTS_DIR / project
    graph_path = project_dir / "graph.json"

    if not graph_path.exists():
        raise click.ClickException(
            f"No graph.json found for project '{project}'. Run 'labyrinth scan' first."
        )

    # Resolve the bundled serve/ directory relative to this source file.
    serve_src = Path(__file__).resolve().parent / "serve"
    if not serve_src.exists():
        raise click.ClickException(f"Dashboard template not found at {serve_src}")

    # Stage files in a temporary serving directory.
    staging_dir = project_dir / ".serve"
    staging_dir.mkdir(exist_ok=True)
    shutil.copytree(serve_src, staging_dir, dirs_exist_ok=True)
    shutil.copy2(graph_path, staging_dir / "graph_data.json")

    # Copy reports.json (or write empty default)
    reports_path = project_dir / "reports.json"
    if reports_path.exists():
        shutil.copy2(reports_path, staging_dir / "reports.json")
    else:
        (staging_dir / "reports.json").write_text(json.dumps({"runs": []}, indent=2) + "\n")

    # Copy heuristics.json (or write empty default)
    heuristics_path = project_dir / "heuristics.json"
    if heuristics_path.exists():
        shutil.copy2(heuristics_path, staging_dir / "heuristics.json")
    else:
        (staging_dir / "heuristics.json").write_text(
            json.dumps({"analyzed_at": None, "graph_generated_at": None, "candidates": []}, indent=2) + "\n"
        )

    handler = partial(SimpleHTTPRequestHandler, directory=str(staging_dir))
    server = HTTPServer(("localhost", port), handler)

    url = f"http://localhost:{port}/index.html"
    click.echo(f"Serving Labyrinth dashboard at {url}")
    click.echo("Press Ctrl+C to stop.")
    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        click.echo("\nStopping server.")
    finally:
        server.server_close()


# ── Agent commands ────────────────────────────────────────────────────


@cli.group()
def agent() -> None:
    """Autonomous agent commands."""


@agent.command()
def analyze() -> None:
    """Run all heuristics against the knowledge graph and save findings."""
    import asyncio

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    project = _get_active_project()
    project_dir = PROJECTS_DIR / project
    graph_path = project_dir / "graph.json"

    if not graph_path.exists():
        raise click.ClickException(
            f"No graph.json found for project '{project}'. Run 'labyrinth scan' first."
        )

    from src.agent.runner import run_analysis

    asyncio.run(run_analysis(project_dir))


@agent.command()
@click.argument("candidate_id")
def run(candidate_id: str) -> None:
    """Execute the agent against a single candidate by its UUID."""
    import asyncio

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    project = _get_active_project()
    project_dir = PROJECTS_DIR / project
    graph_path = project_dir / "graph.json"

    if not graph_path.exists():
        raise click.ClickException(
            f"No graph.json found for project '{project}'. Run 'labyrinth scan' first."
        )

    from src.agent.runner import run_single_candidate

    asyncio.run(run_single_candidate(project_dir, candidate_id))


# ── Config commands ───────────────────────────────────────────────────


@cli.group()
def config() -> None:
    """View and modify global settings."""


@config.command("list")
def config_list() -> None:
    """Show all settings with current values."""
    _maybe_create_labyrinth_dir()
    cfg = _get_config()

    for group in SETTING_GROUPS.values():
        click.echo(f"\n[{group.name}] {group.description}")
        for name in group.settings:
            defn = SETTING_DEFINITIONS[name]
            value = get_setting(cfg, name)
            click.echo(f"  {name} = {value}  ({defn.description})")

    # Show ungrouped settings
    grouped = {n for g in SETTING_GROUPS.values() for n in g.settings}
    ungrouped = [d for d in SETTING_DEFINITIONS.values() if d.name not in grouped]
    for defn in ungrouped:
        value = get_setting(cfg, defn.name)
        click.echo(f"  {defn.name} = {value}  ({defn.description})")


@config.command("get")
@click.argument("name")
def config_get(name: str) -> None:
    """Print the current value of a setting."""
    _maybe_create_labyrinth_dir()
    cfg = _get_config()
    try:
        value = get_setting(cfg, name)
    except KeyError as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(value)


@config.command("set")
@click.argument("name")
@click.argument("value")
def config_set(name: str, value: str) -> None:
    """Set a configuration value."""
    _maybe_create_labyrinth_dir()
    cfg = _get_config()
    try:
        cfg = set_setting(cfg, name, value)
    except (KeyError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    _save_config(cfg)
    click.echo(f"{name} = {get_setting(cfg, name)}")
