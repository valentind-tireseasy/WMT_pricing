"""NLC Model orchestrator — runs the node-level cost computation pipeline."""

import logging
from datetime import datetime

import pandas as pd

from src.data.loader import DataLoader
from src.models.nlc_model import NLCModel

logger = logging.getLogger(__name__)


def run(date_str: str = None, min_units: int = 4, **overrides) -> pd.DataFrame:
    """Run the NLC model for a given date.

    Args:
        date_str: Date string (YYYY-MM-DD). Defaults to today.
        min_units: Minimum inventory units for NLC eligibility.
        **overrides: Additional parameter overrides.

    Returns:
        DataFrame with NLC output for all SKU-Nodes.
    """
    if date_str is None:
        date_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("NLC Model — date=%s, min_units=%d", date_str, min_units)
    logger.info("=" * 60)

    loader = DataLoader()
    try:
        model = NLCModel(date_str=date_str, min_units=min_units, **overrides)
        model.load_data(loader)
        df_output = model.run()

        logger.info("NLC Model complete: %d rows", len(df_output))
        return df_output
    finally:
        loader.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(f"Output: {len(df)} SKU-Node rows")
    print(df["Final node level cost category"].value_counts())
