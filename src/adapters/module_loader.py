"""Load shared modules from the network drive by adding their path to sys.path."""

import sys
from pathlib import Path

import yaml


def _load_settings() -> dict:
    """Load config/settings.yaml relative to the project root."""
    project_root = Path(__file__).resolve().parents[2]
    settings_path = project_root / "config" / "settings.yaml"
    with open(settings_path, "r") as f:
        return yaml.safe_load(f)


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

    Args:
        config_name: Filename (e.g. "settings.yaml", "nlc_model.yaml")
    """
    config_path = get_project_root() / "config" / config_name
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
