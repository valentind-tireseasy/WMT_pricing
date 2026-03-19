"""Load shared modules from the network drive by adding their path to sys.path."""

import sys
from pathlib import Path

import yaml


def _load_raw_settings() -> dict:
    """Load config/settings.yaml without resolving placeholders."""
    project_root = Path(__file__).resolve().parents[2]
    settings_path = project_root / "config" / "settings.yaml"
    with open(settings_path, "r") as f:
        return yaml.safe_load(f)


def _get_drive_letter() -> str:
    """Return the shared drive letter from settings.yaml."""
    settings = _load_raw_settings()
    return settings.get("shared_drive_letter", "G:")


def _resolve_drive(obj, drive: str):
    """Recursively replace {drive} placeholders in strings within a dict/list."""
    if isinstance(obj, str):
        return obj.replace("{drive}", drive)
    if isinstance(obj, dict):
        return {k: _resolve_drive(v, drive) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_drive(item, drive) for item in obj]
    return obj


def _load_settings() -> dict:
    """Load config/settings.yaml with {drive} placeholders resolved."""
    settings = _load_raw_settings()
    drive = settings.get("shared_drive_letter", "G:")
    return _resolve_drive(settings, drive)


def ensure_modules_path() -> str:
    """Add the shared modules directory to sys.path if not already present.

    Returns the resolved modules path.
    """
    settings = _load_settings()
    modules_path = settings["shared_modules_path"]

    if modules_path not in sys.path:
        sys.path.insert(0, modules_path)

    return modules_path


def get_project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parents[2]


def load_yaml(config_name: str) -> dict:
    """Load a YAML file from the config/ directory by name.

    All {drive} placeholders are resolved using the shared_drive_letter
    setting from settings.yaml.

    Args:
        config_name: Filename (e.g. "settings.yaml", "nlc_model.yaml")
    """
    config_path = get_project_root() / "config" / config_name
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    drive = _get_drive_letter()
    return _resolve_drive(data, drive)
