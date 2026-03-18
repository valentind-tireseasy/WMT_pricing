"""Tests tracker updater for Walmart NLC pricing.

Updates the 'Final node level costs tracker.csv' with:
- Refreshed margins and inventory status from model output
- New entries (new SKU-Nodes, price updates, test updates)
- Deduplication by Product Code + Identifier (keep last)

Matches notebook cells 89 (margin update) and 217-218 (tracker save).
"""

import logging
import os
import shutil

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


class TrackerUpdater:
    """Update the NLC tests tracker with price changes.

    Usage:
        updater = TrackerUpdater(df_current_tests, today_str="2026-03-18")
        updater.update_margins(df_output)
        updater.append_entries(list_tracker_dfs)
        updater.save()
    """

    def __init__(self, df_current_tests: pd.DataFrame, today_str: str = None):
        self._settings = load_yaml("settings.yaml")
        self.df_tracker = df_current_tests.copy()
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")
        self._ensure_sku_node_key()

    def _ensure_sku_node_key(self):
        """Ensure SKU-Node column exists."""
        if "SKU-Node" not in self.df_tracker.columns:
            self.df_tracker["SKU-Node"] = (
                self.df_tracker["Product Code"]
                + "-"
                + self.df_tracker["Identifier"].astype(str)
            )

    def update_margins(self, df_output: pd.DataFrame):
        """Update margin columns from model output.

        Matches notebook cell 89:
        - Drops old margin columns
        - Merges fresh values from df_output
        - Sets current_nlc_margin_date to today
        - Fills missing sku_sales_category with "No sales"
        - Adds "Is in stock?" flag
        """
        update_cols = [
            "current_nlc_margin_date",
            "current_nlc_margin",
            "Current walmart margin at NLC category",
            "sku_sales_category",
        ]

        # Drop old values
        existing = [c for c in update_cols if c in self.df_tracker.columns]
        if existing:
            self.df_tracker = self.df_tracker.drop(columns=existing)

        # Merge fresh values
        merge_cols = ["SKU-Node"] + [
            c for c in [
                "current_nlc_margin",
                "Current walmart margin at NLC category",
                "sku_sales_category",
            ]
            if c in df_output.columns
        ]
        self.df_tracker = self.df_tracker.merge(
            df_output[merge_cols], how="left", on="SKU-Node"
        )

        self.df_tracker["current_nlc_margin_date"] = self.today_str
        self.df_tracker["sku_sales_category"] = (
            self.df_tracker["sku_sales_category"]
            .astype(str)
            .replace("nan", "No sales")
        )
        self.df_tracker["Is in stock?"] = np.where(
            self.df_tracker["current_nlc_margin"].isna(), "No", "Yes"
        )

        logger.info("Tracker margins updated.")

    def append_entries(self, list_tracker_dfs: list):
        """Append new/updated tracker entries.

        Matches notebook cell 217:
        - Concatenates all tracker update DataFrames
        - Removes existing rows for updated SKU-Nodes
        - Appends new rows
        - Deduplicates by Product Code + Identifier (keep last)

        Args:
            list_tracker_dfs: List of tracker-format DataFrames
        """
        valid = [df for df in list_tracker_dfs if df is not None and len(df) > 0]
        if not valid:
            logger.info("No tracker entries to append.")
            return

        df_append = pd.concat(valid, ignore_index=True)

        # Ensure SKU-Node key in append data
        if "SKU-Node" not in df_append.columns:
            df_append["SKU-Node"] = (
                df_append["Product Code"]
                + "-"
                + df_append["Identifier"].astype(str)
            )

        # Remove existing rows that are being updated
        df_not_updated = self.df_tracker[
            ~self.df_tracker["SKU-Node"].isin(df_append["SKU-Node"])
        ].copy()

        self.df_tracker = pd.concat(
            [df_not_updated, df_append], ignore_index=True
        )

        # Deduplicate
        dups = self.df_tracker.duplicated(
            subset=["Product Code", "Identifier"], keep=False
        )
        if dups.any():
            logger.warning(
                "Found %d duplicate rows in tracker, keeping last",
                dups.sum(),
            )
            self.df_tracker = self.df_tracker.drop_duplicates(
                subset=["Product Code", "Identifier"], keep="last"
            )

        # Drop helper column before save
        if "SKU-Node" in self.df_tracker.columns:
            self.df_tracker = self.df_tracker.drop(columns=["SKU-Node"])

        logger.info("Tracker updated: %d total rows", len(self.df_tracker))

    def save(self, output_path: str = None, backup: bool = True) -> str:
        """Save the tracker to CSV with backup.

        Matches notebook cell 218.
        """
        nlc_folder = self._settings["shared_paths"]["nlc_folder"]

        if output_path is None:
            output_path = os.path.join(
                nlc_folder, "Final node level costs tracker.csv"
            )

        # Save main tracker
        self.df_tracker.to_csv(output_path, index=False)
        logger.info("Tracker saved: %s (%d rows)", output_path, len(self.df_tracker))

        # Save backup
        if backup:
            bk_folder = os.path.join(nlc_folder, "Bk tracker")
            os.makedirs(bk_folder, exist_ok=True)
            bk_name = f"Final node level costs tracker_{self.today_str}.csv"
            bk_path = os.path.join(bk_folder, bk_name)
            self.df_tracker.to_csv(bk_path, index=False)
            logger.info("Tracker backup: %s", bk_path)

        return output_path
