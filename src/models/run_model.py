"""NLC Model orchestrator — standalone runner for the model step only."""

import logging

import pandas as pd

from src.data.loader import DataLoader
from src.models.nlc_model import NLCModel

logger = logging.getLogger(__name__)


def run(date_str: str = None) -> pd.DataFrame:
    """Run the NLC model for a given date (standalone, without rules/DSV).

    Args:
        date_str: Date string (YYYY-MM-DD). Defaults to today.

    Returns:
        DataFrame with NLC output for all SKU-Nodes.
    """
    if date_str is None:
        date_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    logger.info("NLC Model — date=%s", date_str)

    loader = DataLoader()
    try:
        model = NLCModel(date_str=date_str)
        model.load_data(loader)
        df_output = model.run()

        logger.info("NLC Model complete: %d rows", len(df_output))
        logger.info("Final target distribution:")
        logger.info("\n%s", df_output["Final target"].value_counts().to_string())
        logger.info("Margin category distribution:")
        logger.info(
            "\n%s", df_output["current_nlc_margin category"].value_counts().to_string()
        )

        return df_output
    finally:
        loader.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = run()
    print(f"Output: {len(df)} SKU-Node rows")
