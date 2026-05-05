"""Global settings system for Labyrinth.

Provides a registry of settings with types, defaults, and mutual-exclusion
groups.  Settings are persisted in ``~/.labyrinth/config.toml`` under the
``[settings]`` table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class SettingType(Enum):
    BOOL = "bool"
    STR = "str"


@dataclass
class SettingDefinition:
    name: str
    description: str
    default: bool | str
    setting_type: SettingType
    group: str | None = None


@dataclass
class SettingGroup:
    name: str
    description: str
    settings: list[str] = field(default_factory=list)


# ── Registry ──────────────────────────────────────────────────────────

SETTING_DEFINITIONS: dict[str, SettingDefinition] = {}
SETTING_GROUPS: dict[str, SettingGroup] = {}


def _register_group(name: str, description: str) -> None:
    SETTING_GROUPS[name] = SettingGroup(name=name, description=description)


def _register_setting(
    name: str,
    description: str,
    default: bool | str,
    setting_type: SettingType,
    group: str | None = None,
) -> None:
    SETTING_DEFINITIONS[name] = SettingDefinition(
        name=name,
        description=description,
        default=default,
        setting_type=setting_type,
        group=group,
    )
    if group and group in SETTING_GROUPS:
        SETTING_GROUPS[group].settings.append(name)


# ── Plugin-enable-mode group ─────────────────────────────────────────

_register_group(
    "plugin-enable-mode",
    "Controls how codebase plugins are enabled during scans.",
)

_register_setting(
    "auto-enable-all-plugins",
    "Enable all available plugins for every codebase target.",
    default=True,
    setting_type=SettingType.BOOL,
    group="plugin-enable-mode",
)

_register_setting(
    "auto-enable-relevant-plugins",
    "Auto-detect which plugins are relevant based on project dependencies.",
    default=False,
    setting_type=SettingType.BOOL,
    group="plugin-enable-mode",
)

_register_setting(
    "manually-enable-plugins",
    "Only use plugins explicitly configured on each target.",
    default=False,
    setting_type=SettingType.BOOL,
    group="plugin-enable-mode",
)


# ── Helpers ───────────────────────────────────────────────────────────

def _parse_bool(value: str) -> bool:
    if value.lower() in ("true", "1", "yes"):
        return True
    if value.lower() in ("false", "0", "no"):
        return False
    raise ValueError(f"Cannot parse '{value}' as boolean. Use true/false.")


def get_setting(config: dict, name: str) -> bool | str:
    """Get a setting value, falling back to its default."""
    defn = SETTING_DEFINITIONS.get(name)
    if defn is None:
        raise KeyError(f"Unknown setting: {name}")
    settings = config.get("settings", {})
    return settings.get(name, defn.default)


def set_setting(config: dict, name: str, raw_value: str) -> dict:
    """Set a setting value, enforcing mutual exclusion within groups.

    Returns the updated config dict.
    """
    defn = SETTING_DEFINITIONS.get(name)
    if defn is None:
        raise KeyError(f"Unknown setting: {name}")

    if defn.setting_type is SettingType.BOOL:
        value = _parse_bool(raw_value)
    else:
        value = raw_value

    config.setdefault("settings", {})

    # Enforce mutual exclusion: if setting a group member to True,
    # set all other members to False.
    if defn.group and value is True:
        group = SETTING_GROUPS[defn.group]
        for sibling in group.settings:
            config["settings"][sibling] = False

    config["settings"][name] = value
    return config


def get_plugin_enable_mode(config: dict) -> str:
    """Return the name of the active plugin-enable-mode setting."""
    if get_setting(config, "auto-enable-all-plugins"):
        return "auto-enable-all-plugins"
    if get_setting(config, "auto-enable-relevant-plugins"):
        return "auto-enable-relevant-plugins"
    return "manually-enable-plugins"
