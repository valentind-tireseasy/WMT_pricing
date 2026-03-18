"""Walmart NLC pricing rules engine.

Categorizes SKU-Node rows from the NLC model output into update types
and computes final prices for each category:
- New SKU-Nodes (added to DSV)
- Low price updates (margin correction upward)
- High price updates (margin correction downward to 20%)
- Walmart margin split test updates
- Brand margin test updates
- Less sales adjustments
"""

import logging

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


class PricingRulesEngine:
    """Apply pricing rules to NLC model output.

    Usage:
        engine = PricingRulesEngine(df_output, df_current_tests)
        df_new_nodes = engine.get_new_sku_nodes()
        df_low_updates = engine.get_low_price_updates()
        df_high_updates = engine.get_high_price_updates()
        df_wm_split = engine.get_wm_margin_split_updates()
        df_margin_test = engine.get_margin_test_updates()
    """

    def __init__(self, df_output: pd.DataFrame, df_current_tests: pd.DataFrame):
        self._config = load_yaml("pricing_rules.yaml")
        self.df_output = df_output
        self.df_current_tests = df_current_tests

    def get_new_sku_nodes(self) -> pd.DataFrame:
        """Get SKU-Nodes that are new (not in current tests tracker)."""
        df = self.df_output[self.df_output["New NLC"] == "Yes"].copy()
        df["Final target"] = "Added"
        df["Final price change category final"] = "New"

        cols = [
            "Product Code", "Identifier", "Final node level cost",
            "Final node level cost category", "Node type", "Min units",
            "SKU-Node", "Final target", "Final price change category final",
        ]
        return df[[c for c in cols if c in df.columns]]

    def get_low_price_updates(self) -> pd.DataFrame:
        """Get SKU-Nodes needing price increases (margin too low)."""
        config = self._config["rules"]["low_price_updates"]
        margin_target = config["margin_target"]

        df = self.df_output[
            (self.df_output["New NLC"] == "No") &
            (self.df_output["current_nlc_margin"] < margin_target) &
            (self.df_output["current_nlc_margin"].notna())
        ].copy()

        pct_label = f"{int(margin_target * 100)}%"
        price_col = f"Node level cost - {pct_label} margin"

        if price_col in df.columns:
            df["Final price"] = df[price_col]
        else:
            df["Final price"] = df["Final node level cost"]

        df["Final target"] = config["target_label"]
        df["Final price change category final"] = "Increase"

        logger.info("Low price updates: %d SKU-Nodes", len(df))
        return df

    def get_high_price_updates(self) -> pd.DataFrame:
        """Get SKU-Nodes needing price decreases (margin above 20%)."""
        config = self._config["rules"]["high_price_updates"]
        margin_target = config["margin_target"]

        df = self.df_output[
            (self.df_output["New NLC"] == "No") &
            (self.df_output["current_nlc_margin"] > margin_target)
        ].copy()

        pct_label = f"{int(margin_target * 100)}%"
        price_col = f"Node level cost - {pct_label} margin"

        if price_col in df.columns:
            df["Final price"] = df[price_col]
        else:
            df["Final price"] = df["Final node level cost"]

        df["Final target"] = config["target_label"]
        df["Final price change category final"] = "Decrease"

        logger.info("High price updates: %d SKU-Nodes", len(df))
        return df

    def get_wm_margin_split_updates(self, today_str: str = None) -> pd.DataFrame:
        """Get updates for SKU-Nodes in the Walmart margin split test."""
        df = self.df_output[
            self.df_output["Final target"] == "Wm margin split test"
        ].copy()

        if len(df) == 0:
            # Fall back to tests tracker
            wm_tests = self.df_current_tests[
                self.df_current_tests["Final target"] == "Wm margin split test"
            ].copy()
            if len(wm_tests) == 0:
                logger.info("No Walmart margin split test SKU-Nodes found.")
                return pd.DataFrame()

            wm_tests["SKU-Node"] = (
                wm_tests["Product Code"] + "-" + wm_tests["Identifier"].astype(str)
            )
            df = self.df_output.merge(
                wm_tests[["SKU-Node", "Sub-group"]], on="SKU-Node", how="inner"
            )

        if len(df) == 0:
            return pd.DataFrame()

        config = self._config["rules"]["wm_margin_split"]
        sub_groups = {sg["name"]: sg["margin_split"] for sg in config["sub_groups"]}

        # Compute price per sub-group
        for sg_name, split in sub_groups.items():
            mask = df["Sub-group"] == sg_name
            if "offer_price" in df.columns and "Purchase Price+FET" in df.columns:
                wm_margin = df["offer_price"] - df["Purchase Price+FET"]
                df.loc[mask, "Price"] = df.loc[mask, "Purchase Price+FET"] + (
                    wm_margin * split
                )
            else:
                df.loc[mask, "Price"] = df.loc[mask, "Final node level cost"]

        df["Final target"] = "Wm margin split test"

        logger.info("Walmart margin split updates: %d SKU-Nodes", len(df))
        return df

    def get_margin_test_updates(self, start_dates: list = None) -> pd.DataFrame:
        """Get updates for SKU-Nodes in brand margin tests."""
        margin_tests = self.df_current_tests[
            self.df_current_tests["Final target"] == "Margin test"
        ].copy()

        if start_dates:
            margin_tests = margin_tests[
                margin_tests["Start date"].isin(start_dates)
            ]

        if len(margin_tests) == 0:
            logger.info("No margin test SKU-Nodes found.")
            return pd.DataFrame()

        margin_tests["SKU-Node"] = (
            margin_tests["Product Code"] + "-" + margin_tests["Identifier"].astype(str)
        )

        df = self.df_output.merge(
            margin_tests[["SKU-Node", "Sub-group"]], on="SKU-Node", how="inner"
        )

        if len(df) == 0:
            return pd.DataFrame()

        # Compute price based on sub-group margin target
        for sg in self._config["rules"]["margin_test"]["sub_groups"]:
            margin_val = int(sg.replace("%", "")) / 100.0
            mask = df["Sub-group"] == sg
            df.loc[mask, "Price"] = df.loc[mask, "Purchase Price+FET"] / (1 - margin_val)

        df["Final target"] = "Margin test"

        logger.info("Margin test updates: %d SKU-Nodes", len(df))
        return df
