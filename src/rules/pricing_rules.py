"""Walmart NLC pricing rules engine.

Categorizes SKU-Node rows from the NLC model output into update types
and computes final prices for each category. Matches the notebook logic exactly.

Update categories (in order of application):
1. Wm margin split test updates (from tracker)
2. Brand margin test updates (from tracker)
3. Low price updates (current_nlc_margin < 5.9%)
4. High price updates (current_nlc_margin > 20.3%)
5. Price increase test updates (from tracker)
6. DSVD shipping cost test updates (from tracker)
7. New SKU-Nodes (not in current DSV)
"""

import logging

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


class PricingRulesEngine:
    """Apply pricing rules to NLC model output.

    Usage:
        engine = PricingRulesEngine(df_output, df_current_tests, today_str)
        results = engine.run_all()
    """

    def __init__(self, df_output: pd.DataFrame, df_current_tests: pd.DataFrame,
                 today_str: str, test_mode: bool = False):
        self._config = load_yaml("pricing_rules.yaml")
        self._nlc_config = load_yaml("nlc_model.yaml")
        self.df_output = df_output
        self.df_current_tests = df_current_tests
        self.today_str = today_str
        self.test_mode = test_mode
        self.min_price_change_pct = self._config["min_price_change_pct"]
        self.dont_update_targets = self._nlc_config["dont_update_targets"]
        self.min_margin = self._nlc_config["min_margin_update_prices"]
        self.max_margin = self._nlc_config["max_margin_update_prices"]

    def get_wm_margin_split_updates(self) -> tuple:
        """Get updates for SKU-Nodes in the Walmart margin split test.

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        df = self.df_output[
            self.df_output["Final target"] == "Wm margin split test"
        ].copy()

        if len(df) == 0:
            logger.info("No Walmart margin split test SKU-Nodes found.")
            return pd.DataFrame(), pd.DataFrame()

        # Compute price per sub-group
        # 60% split → Price margin split 60%
        # 50% split → Price margin split 50%
        # Baseline → Final node level cost on increase, else current_nlc_price
        df["Price"] = np.where(
            df["Sub-group"] == "60% split",
            df["Price margin split 60%"],
            np.where(
                df["Sub-group"] == "50% split",
                df["Price margin split 50%"],
                np.where(
                    (df["Sub-group"] == "Baseline")
                    & (df["Final price change category"] == "Increase"),
                    df["Final node level cost"],
                    df["current_nlc_price"],
                ),
            ),
        )

        df["Price change %"] = round(
            (df["Price"] - df["current_nlc_price"]) / df["current_nlc_price"], 4
        )
        df["Price change category"] = np.where(
            df["Price change %"] < 0,
            "Decrease",
            np.where(df["Price change %"] > 0, "Increase", "No change"),
        )

        # Only update where |delta| >= 1%
        df_update = df[abs(df["Price change %"]) >= self.min_price_change_pct].copy()

        logger.info("Wm margin split updates: %d SKU-Nodes", len(df_update))

        # DSV format
        df_dsv = df_update[
            ["Product Code", "Identifier", "Price", "SKU-Node"]
        ].rename(columns={"Product Code": "SKU", "Identifier": "Source"})

        # Tracker format
        df_tracker = df_update[["SKU-Node"]].merge(
            self.df_current_tests, on="SKU-Node", how="left"
        )
        df_tracker["Last price update"] = self.today_str

        return df_dsv, df_tracker

    def get_margin_test_updates(self, start_dates: list = None) -> tuple:
        """Get updates for SKU-Nodes in brand margin tests.

        Args:
            start_dates: Filter to specific test start dates (e.g. ["2026-03-12"])

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        df = self.df_output[
            self.df_output["Final target"] == "Margin test"
        ].copy()

        if start_dates:
            df = df[df["Start date"].isin(start_dates)].copy()

        if len(df) == 0:
            logger.info("No margin test SKU-Nodes found.")
            return pd.DataFrame(), pd.DataFrame()

        # Compute price based on sub-group margin %
        # Each sub-group like "11%" → use "Node level cost - 11% margin"
        sub_groups = self._config["rules"]["margin_test"]["sub_groups"]
        conditions = []
        choices = []
        for sg in sub_groups:
            col = f"Node level cost - {sg} margin"
            if col in df.columns:
                conditions.append(df["Sub-group"] == sg)
                choices.append(df[col])

        df["Price"] = np.select(conditions, choices, default=df["current_nlc_price"])

        df["Price change %"] = round(
            (df["Price"] - df["current_nlc_price"]) / df["current_nlc_price"], 4
        )
        df["Price change category"] = np.where(
            df["Price change %"] < 0,
            "Decrease",
            np.where(df["Price change %"] > 0, "Increase", "No change"),
        )

        # Only update where |delta| >= 1%
        df_update = df[abs(df["Price change %"]) >= self.min_price_change_pct].copy()

        logger.info("Margin test updates: %d SKU-Nodes", len(df_update))

        # DSV format
        df_dsv = df_update[
            ["Product Code", "Identifier", "Price", "SKU-Node"]
        ].rename(columns={"Product Code": "SKU", "Identifier": "Source"})

        # Tracker format
        df_tracker = df_update[["SKU-Node"]].merge(
            self.df_current_tests, on="SKU-Node", how="left"
        )
        df_tracker["Last price update"] = self.today_str

        return df_dsv, df_tracker

    def get_low_price_updates(self) -> tuple:
        """Get SKU-Nodes needing price increases (margin < 5.9%).

        Uses Final node level cost (from 11%→8%→6% cascade).
        Excludes: Margin test, Wm margin split test, Shipping cost added.

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        cols = [
            "Product Code", "Identifier", "Purchase Price+FET",
            "Node level cost - 6% margin", "Final target",
            "Final node level cost", "Final node level cost category",
            "Node type", "Min units", "current_nlc_margin",
        ]
        available_cols = [c for c in cols if c in self.df_output.columns]
        df = self.df_output[available_cols].copy()

        # Filter: margin below threshold AND not in protected targets
        df = df[
            (df["current_nlc_margin"] < self.min_margin)
            & (~df["Final target"].isin(self.dont_update_targets))
        ].copy()

        # Categorize
        df["Category inventory"] = np.where(
            df["current_nlc_margin"] < 0.04,
            "Not showing inventory",
            "Below 6% margin",
        )
        df["SKU-Node"] = (
            df["Product Code"] + "-" + df["Identifier"].astype(str)
        )

        logger.info("Low price updates: %d SKU-Nodes", len(df))

        # DSV format: use Final node level cost as new price
        df_dsv = df[
            ["Product Code", "Identifier", "Final node level cost"]
        ].rename(
            columns={
                "Identifier": "Source",
                "Final node level cost": "Price",
                "Product Code": "SKU",
            }
        )
        df_dsv["SKU-Node"] = df_dsv["SKU"] + "-" + df_dsv["Source"].astype(str)

        # Tracker format
        tracker_cols = [
            "Product Code", "Identifier", "Final node level cost",
            "Final node level cost category", "Category inventory",
            "Node type", "Min units",
        ]
        available_tracker = [c for c in tracker_cols if c in df.columns]
        df_tracker = df[available_tracker].copy()
        df_tracker["Final target"] = "Updated"
        df_tracker = df_tracker.rename(columns={
            "Final node level cost category": "Final price category",
            "Final node level cost": "Final price",
        })
        df_tracker["SKU-Node"] = (
            df_tracker["Product Code"] + "-" + df_tracker["Identifier"].astype(str)
        )
        df_tracker["Min units"] = df_tracker["Min units"].astype(int)
        df_tracker["Start date"] = self.today_str

        return df_dsv, df_tracker

    def get_high_price_updates(self) -> tuple:
        """Get SKU-Nodes needing price decreases (margin > 20.3%).

        Uses Node level cost - 20% margin as the target price.
        Excludes: Margin test, Wm margin split test, Shipping cost added.

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        cols = [
            "Product Code", "Identifier", "Purchase Price+FET",
            "Node level cost - 20% margin", "Node type", "Min units",
            "current_nlc_margin", "current_nlc_price", "Final target",
            "Target for node level cost? - 20% margin", "Sub-group",
        ]
        available_cols = [c for c in cols if c in self.df_output.columns]
        df = self.df_output[available_cols].copy()

        df = df[df["current_nlc_margin"] > self.max_margin].copy()
        df["Category inventory"] = ""
        df["SKU-Node"] = (
            df["Product Code"] + "-" + df["Identifier"].astype(str)
        )

        # Exclude protected targets
        df = df[~df["Final target"].isin(self.dont_update_targets)].copy()

        # Delta vs current price
        df["Final delta vs current %"] = round(
            (df["Node level cost - 20% margin"] - df["current_nlc_price"])
            / df["current_nlc_price"],
            4,
        )
        df["Final price change category final"] = np.where(
            df["Final delta vs current %"] < 0,
            "Decrease",
            np.where(df["Final delta vs current %"] > 0, "Increase", "No change"),
        )

        logger.info("High price updates: %d SKU-Nodes", len(df))

        # DSV format
        df_dsv = df[
            ["Product Code", "Identifier", "Node level cost - 20% margin"]
        ].rename(
            columns={
                "Identifier": "Source",
                "Node level cost - 20% margin": "Price",
                "Product Code": "SKU",
            }
        )
        df_dsv["SKU-Node"] = df_dsv["SKU"] + "-" + df_dsv["Source"].astype(str)

        # Tracker format
        tracker_cols = [
            "Product Code", "Identifier", "Node level cost - 20% margin",
            "Category inventory", "Node type", "Min units",
            "Final delta vs current %", "Final price change category final",
            "Final target", "SKU-Node", "Sub-group",
        ]
        available_tracker = [c for c in tracker_cols if c in df.columns]
        df_tracker = df[available_tracker].copy()

        # Preserve existing target label for margin test / split test rows
        df_tracker["Final target"] = np.where(
            df_tracker["Final target"] == "Wm margin split test",
            "Wm margin split test",
            np.where(
                df_tracker["Final target"] == "Margin test",
                "Margin test",
                "Decreased - margin > 20%",
            ),
        )
        df_tracker = df_tracker.rename(columns={
            "Node level cost - 20% margin": "Final price",
        })
        df_tracker["Final price category"] = "20%"
        df_tracker["Min units"] = df_tracker["Min units"].astype(int)
        df_tracker["Start date"] = self.today_str

        return df_dsv, df_tracker

    def get_price_increase_test_updates(self) -> tuple:
        """Get updates for SKU-Nodes in the price increase test.

        Logic (from live notebook cell 176):
        - Sub-group "Increased" → use Final node level cost
        - Else if current_nlc_margin < 6% → use Final node level cost
        - Else → keep current_nlc_price

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        df = self.df_output[
            self.df_output["Final target"] == "Increase test"
        ].copy()

        if len(df) == 0:
            logger.info("No price increase test SKU-Nodes found.")
            return pd.DataFrame(), pd.DataFrame()

        df["Price"] = np.where(
            df["Sub-group"] == "Increased",
            df["Final node level cost"],
            np.where(
                df["current_nlc_margin"] < 0.06,
                df["Final node level cost"],
                df["current_nlc_price"],
            ),
        )

        df["Price change %"] = round(
            (df["Price"] - df["current_nlc_price"]) / df["current_nlc_price"], 4
        )
        df["Price change category"] = np.where(
            df["Price change %"] < 0,
            "Decrease",
            np.where(df["Price change %"] > 0, "Increase", "No change"),
        )

        # Only update where |delta| >= 1%
        df_update = df[abs(df["Price change %"]) >= self.min_price_change_pct].copy()

        logger.info("Price increase test updates: %d SKU-Nodes", len(df_update))

        # DSV format
        df_dsv = df_update[
            ["Product Code", "Identifier", "Price", "SKU-Node"]
        ].rename(columns={"Product Code": "SKU", "Identifier": "Source"})

        # Tracker format
        df_tracker = df_update[["SKU-Node"]].merge(
            self.df_current_tests, on="SKU-Node", how="left"
        )
        df_tracker["Last price update"] = self.today_str

        return df_dsv, df_tracker

    def get_dsvd_test_updates(self, df_dsvd_test: pd.DataFrame) -> tuple:
        """Get updates for SKU-Nodes in the DSVD shipping cost test.

        Logic (from live notebook cells 180-183):
        - Sub-group "No shipping" → Final node level cost
        - Sub-group "Shipping cost added" → Final node level cost + Shipping cost DSVD
        - Otherwise → current_nlc_price

        Args:
            df_dsvd_test: DataFrame with columns [Identifier, Shipping cost DSVD]

        Returns:
            (df_dsv, df_tracker) — DSV-format updates and tracker updates
        """
        df = self.df_output[
            self.df_output["Final target"] == "DSVD test"
        ].copy()

        if len(df) == 0:
            logger.info("No DSVD test SKU-Nodes found.")
            return pd.DataFrame(), pd.DataFrame()

        df = df.merge(df_dsvd_test, how="left", on="Identifier")

        df["Price"] = np.where(
            df["Sub-group"] == "No shipping",
            df["Final node level cost"],
            np.where(
                df["Sub-group"] == "Shipping cost added",
                df["Final node level cost"] + df["Shipping cost DSVD"],
                df["current_nlc_price"],
            ),
        )

        df["Price change %"] = round(
            (df["Price"] - df["current_nlc_price"]) / df["current_nlc_price"], 4
        )
        df["Price change category"] = np.where(
            df["Price change %"] < 0,
            "Decrease",
            np.where(df["Price change %"] > 0, "Increase", "No change"),
        )

        # Only update where |delta| >= 1%
        df_update = df[abs(df["Price change %"]) >= self.min_price_change_pct].copy()

        logger.info("DSVD test updates: %d SKU-Nodes", len(df_update))

        # DSV format
        df_dsv = df_update[
            ["Product Code", "Identifier", "Price", "SKU-Node"]
        ].rename(columns={"Product Code": "SKU", "Identifier": "Source"})

        # Tracker format
        df_tracker = df_update[["SKU-Node"]].merge(
            self.df_current_tests, on="SKU-Node", how="left"
        )
        df_tracker["Last price update"] = self.today_str

        return df_dsv, df_tracker

    def get_new_sku_nodes(self) -> tuple:
        """Get SKU-Nodes that are new (not in current DSV).

        Returns:
            (df_dsv, df_tracker) — DSV-format and tracker-format DataFrames
        """
        df = self.df_output[self.df_output["New NLC"] == "Yes"].copy()
        df = df[
            ["Product Code", "Identifier", "Final node level cost",
             "Final node level cost category", "Node type", "Min units"]
        ].copy()
        df["Final target"] = "Added"
        df["Final price change category final"] = "Added"
        df["SKU-Node"] = (
            df["Product Code"] + "-" + df["Identifier"].astype(str)
        )

        logger.info("New SKU-Nodes: %d", len(df))

        # DSV format
        df_dsv = df.rename(columns={
            "Product Code": "SKU",
            "Final node level cost": "Price",
            "Identifier": "Source",
        }).drop(columns=["Final node level cost category"], errors="ignore")

        # Tracker format
        df_tracker = df.rename(columns={
            "Final node level cost category": "Final price category",
            "Final node level cost": "Final price",
        })
        df_tracker["Start date"] = self.today_str

        return df_dsv, df_tracker
