"""Price update categorization and final price assignment.

Takes the NLC model output + rules engine results and produces
the list of price updates to apply to the DSV file.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class PriceUpdateManager:
    """Manage the various price update categories and merge them.

    Usage:
        manager = PriceUpdateManager(df_output, df_current_tests, today_str)
        manager.add_updates("wm_margin_split", df_wm_split)
        manager.add_updates("margin_test", df_margin_test)
        manager.add_updates("low_price", df_low_updates)
        manager.add_updates("high_price", df_high_updates)
        manager.add_new_sku_nodes(df_new_nodes)
        df_all_updates, df_new_nodes_final = manager.get_all()
    """

    def __init__(self, df_output: pd.DataFrame, df_current_tests: pd.DataFrame,
                 today_str: str):
        self.df_output = df_output
        self.df_current_tests = df_current_tests
        self.today_str = today_str
        self._update_dfs = {}
        self._new_nodes_df = None

    def add_updates(self, name: str, df: pd.DataFrame):
        """Register a price update DataFrame."""
        if df is not None and len(df) > 0:
            required_cols = ["Product Code", "Identifier", "Price"]
            # Rename common price columns to "Price"
            for alt in ["Final price", "Final node level cost", "Test price"]:
                if alt in df.columns and "Price" not in df.columns:
                    df = df.rename(columns={alt: "Price"})
                    break

            self._update_dfs[name] = df
            logger.info("Added %d updates from '%s'", len(df), name)

    def add_new_sku_nodes(self, df: pd.DataFrame):
        """Register new SKU-Node additions."""
        if df is not None and len(df) > 0:
            self._new_nodes_df = df
            logger.info("Added %d new SKU-Nodes", len(df))

    def get_all_updates_dsv(self) -> pd.DataFrame:
        """Merge all update DataFrames into DSV format (SKU, Price, Source).

        Returns:
            DataFrame with columns: SKU, Price, Source, SKU-Node
        """
        dfs = []
        for name, df in self._update_dfs.items():
            df_dsv = df[["Product Code", "Identifier", "Price"]].copy()
            df_dsv = df_dsv.rename(columns={
                "Product Code": "SKU",
                "Identifier": "Source",
            })
            df_dsv["SKU-Node"] = df_dsv["SKU"] + "-" + df_dsv["Source"].astype(str)
            dfs.append(df_dsv)

        if not dfs:
            return pd.DataFrame(columns=["SKU", "Price", "Source", "SKU-Node"])

        df_all = pd.concat(dfs, ignore_index=True)

        # Deduplicate: keep last (later updates override earlier ones)
        df_all = df_all.drop_duplicates(subset=["SKU-Node"], keep="last")

        logger.info("Total update rows (deduplicated): %d", len(df_all))
        return df_all

    def get_new_nodes_dsv(self) -> pd.DataFrame:
        """Get new SKU-Nodes in DSV format."""
        if self._new_nodes_df is None or len(self._new_nodes_df) == 0:
            return pd.DataFrame(columns=["SKU", "Price", "Source", "SKU-Node"])

        df = self._new_nodes_df.copy()

        # Map column names to DSV format
        rename_map = {}
        if "Product Code" in df.columns:
            rename_map["Product Code"] = "SKU"
        if "Final node level cost" in df.columns and "Price" not in df.columns:
            rename_map["Final node level cost"] = "Price"
        if "Identifier" in df.columns:
            rename_map["Identifier"] = "Source"

        df = df.rename(columns=rename_map)

        if "SKU-Node" not in df.columns:
            df["SKU-Node"] = df["SKU"] + "-" + df["Source"].astype(str)

        return df[["SKU", "Price", "Source", "SKU-Node"]]

    def get_tracker_updates(self) -> pd.DataFrame:
        """Get updates formatted for the tests tracker."""
        tracker_rows = []

        # New SKU-Nodes
        if self._new_nodes_df is not None and len(self._new_nodes_df) > 0:
            df = self._new_nodes_df.copy()
            if "Final target" not in df.columns:
                df["Final target"] = "Added"
            if "Start date" not in df.columns:
                df["Start date"] = self.today_str
            if "Last price update" not in df.columns:
                df["Last price update"] = self.today_str
            tracker_rows.append(df)

        # Regular updates — update Last price update date
        for name, df in self._update_dfs.items():
            df_tracker = df.copy()
            df_tracker["Last price update"] = self.today_str
            tracker_rows.append(df_tracker)

        if not tracker_rows:
            return pd.DataFrame()

        return pd.concat(tracker_rows, ignore_index=True)
