"""Tests tracker updater for Walmart NLC pricing.

Updates the 'Final node level costs tracker.csv' with new entries
and refreshed margin/inventory data for existing entries.
"""

import logging
import os
import shutil
from datetime import datetime

import pandas as pd

from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


class TrackerUpdater:
    """Update the NLC tests tracker with price changes.

    Usage:
        updater = TrackerUpdater(df_current_tracker, today_str="2026-03-18")
        updater.update_margins(df_output)
        updater.append_new_entries(df_tracker_updates)
        updater.save()
    """

    def __init__(self, df_current_tracker: pd.DataFrame, today_str: str = None):
        self._settings = load_yaml("settings.yaml")
        self.df_tracker = df_current_tracker.copy()
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")
        self._ensure_sku_node_key()

    def _ensure_sku_node_key(self):
        """Ensure SKU-Node column exists."""
        if "SKU-Node" not in self.df_tracker.columns:
            self.df_tracker["SKU-Node"] = (
                self.df_tracker["Product Code"] + "-"
                + self.df_tracker["Identifier"].astype(str)
            )

    def update_margins(self, df_output: pd.DataFrame):
        """Update current NLC margin and inventory data from model output.

        Args:
            df_output: NLC model output DataFrame with margin calculations.
        """
        update_cols = [
            "current_nlc_margin_date",
            "current_nlc_margin",
            "Current walmart margin at NLC category",
            "sku_sales_category",
        ]

        # Drop old values of update columns
        existing_cols = [c for c in update_cols if c in self.df_tracker.columns]
        if existing_cols:
            self.df_tracker = self.df_tracker.drop(columns=existing_cols)

        # Merge fresh values from model output
        merge_cols = ["SKU-Node"] + [c for c in update_cols if c in df_output.columns]
        if len(merge_cols) > 1:
            self.df_tracker = self.df_tracker.merge(
                df_output[merge_cols], how="left", on="SKU-Node"
            )

        logger.info("Tracker margins updated from model output.")

    def append_new_entries(self, df_new_entries: pd.DataFrame):
        """Append new tracker rows (new SKU-Nodes + updated entries).

        Existing SKU-Node rows with the same SKU-Node key get their
        'Last price update' refreshed rather than being duplicated.
        """
        if df_new_entries is None or len(df_new_entries) == 0:
            return

        # Ensure SKU-Node key
        if "SKU-Node" not in df_new_entries.columns:
            df_new_entries["SKU-Node"] = (
                df_new_entries["Product Code"] + "-"
                + df_new_entries["Identifier"].astype(str)
            )

        # Split: truly new vs existing
        existing_nodes = set(self.df_tracker["SKU-Node"])
        df_truly_new = df_new_entries[
            ~df_new_entries["SKU-Node"].isin(existing_nodes)
        ].copy()

        df_existing_updates = df_new_entries[
            df_new_entries["SKU-Node"].isin(existing_nodes)
        ].copy()

        # For existing: update the Last price update date
        if len(df_existing_updates) > 0:
            update_map = df_existing_updates.set_index("SKU-Node")[
                "Last price update"
            ].to_dict()
            mask = self.df_tracker["SKU-Node"].isin(update_map)
            self.df_tracker.loc[mask, "Last price update"] = (
                self.df_tracker.loc[mask, "SKU-Node"].map(update_map)
            )
            logger.info("Updated %d existing tracker entries", len(df_existing_updates))

        # For new: append
        if len(df_truly_new) > 0:
            self.df_tracker = pd.concat(
                [self.df_tracker, df_truly_new], ignore_index=True
            )
            logger.info("Appended %d new tracker entries", len(df_truly_new))

    def save(self, output_path: str = None, backup: bool = True) -> str:
        """Save the tracker to CSV, with optional backup.

        Args:
            output_path: Override path. Defaults to shared drive location.
            backup: If True, save a backup copy before overwriting.

        Returns:
            Path the tracker was saved to.
        """
        nlc_folder = self._settings["shared_paths"]["nlc_folder"]

        if output_path is None:
            output_path = os.path.join(
                nlc_folder, "Final node level costs tracker.csv"
            )

        if backup and os.path.exists(output_path):
            bk_folder = os.path.join(nlc_folder, "Bk tracker")
            os.makedirs(bk_folder, exist_ok=True)
            bk_name = f"Final node level costs tracker_bk_{self.today_str}.csv"
            bk_path = os.path.join(bk_folder, bk_name)
            shutil.copy2(output_path, bk_path)
            logger.info("Tracker backup saved: %s", bk_path)

        self.df_tracker.to_csv(output_path, index=False)
        logger.info("Tracker saved: %s (%d rows)", output_path, len(self.df_tracker))
        return output_path
