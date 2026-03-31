"""Load and validate correlation analysis configuration."""

from src.adapters.module_loader import load_yaml


def load_analysis_config() -> dict:
    """Load config/correlation_analysis.yaml with defaults applied.

    Returns the full config dict.  Every analysis module receives the
    relevant sub-dict from the caller (the notebook).
    """
    cfg = load_yaml("correlation_analysis.yaml")

    # Ensure CI defaults exist
    ci = cfg.setdefault("ci", {})
    ci.setdefault("level", 0.95)
    ci.setdefault("n_bootstrap", 2000)
    ci.setdefault("seed", 42)

    return cfg
