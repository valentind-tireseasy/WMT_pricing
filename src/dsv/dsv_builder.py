"""DSV file builder for Walmart B2B.

Takes the current DSV file, applies price updates (from rules engine),
adds new SKU-Nodes, handles rollbacks, and generates the final DSV CSV.

Matches the notebook's DSV construction flow (cells 192-215):
1. Start from current DSV (renamed to SKU/Price/Source)
2. Split into NLC (has Source) and National (no Source)
3. Remove rollback SKUs from NLC rows
4. Apply rollback prices to national rows
5. Remove rows being updated, concat updates + new nodes
6. Add "Minimum margin" column, deduplicate
7. Save and validate
"""

import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


class DSVBuilder:
    """Build the new DSV file from current DSV + price updates.

    Usage:
        builder = DSVBuilder(df_curr_dsv_original, df_rollbacks, today_str)
        df_new_dsv = builder.build(
            list_dsv_updates=[df_wm_split_dsv, df_margin_dsv, df_low_dsv, df_high_dsv],
            df_new_nodes=df_new_nodes_dsv,
        )
        builder.save(df_new_dsv)
    """

    def __init__(self, df_curr_dsv_original: pd.DataFrame,
                 df_rollbacks: pd.DataFrame = None,
                 today_str: str = None):
        self._config = load_yaml("nlc_model.yaml")
        self._settings = load_yaml("settings.yaml")
        self.df_curr_dsv_original = df_curr_dsv_original
        self.df_rollbacks = df_rollbacks
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")
        self.minimum_margin = self._config["dsv"]["minimum_margin"]

    def build(self, list_dsv_updates: list, df_new_nodes: pd.DataFrame) -> pd.DataFrame:
        """Build the new DSV by applying all updates.

        Args:
            list_dsv_updates: List of DSV-format DataFrames (each has SKU, Price, Source, SKU-Node)
            df_new_nodes: New SKU-Node DSV DataFrame

        Returns:
            Final DSV DataFrame with columns: SKU, Price, Minimum margin, Source
        """
        # Step 1: Prepare starting DSV
        df_start = self.df_curr_dsv_original.copy()
        df_start = df_start.rename(columns={
            "sku": "SKU",
            "walmart_dsv_price": "Price",
            "source": "Source",
        })
        df_start["SKU-Node"] = (
            df_start["SKU"] + "-" + df_start["Source"].fillna("").astype(str)
        )

        original_rows = len(df_start)
        logger.info("Starting DSV: %d rows", original_rows)

        # Step 2: Split NLC vs National
        df_nlc = df_start[df_start["Source"].notna()].copy()
        df_national = df_start[df_start["Source"].isna()].copy()

        # Step 3: Remove rollback SKUs from NLC rows
        if self.df_rollbacks is not None and len(self.df_rollbacks) > 0:
            rollback_skus = self.df_rollbacks["Product Code"].unique()
            before = len(df_nlc)
            df_nlc = df_nlc[~df_nlc["SKU"].isin(rollback_skus)].copy()
            logger.info(
                "Removed %d rollback SKU rows from NLC nodes", before - len(df_nlc)
            )

            # Step 4: Apply rollback prices to national rows
            df_rb_prices = (
                self.df_rollbacks.groupby("Product Code")
                .agg({"Unit cost": "min"})
                .reset_index()
                .rename(columns={"Product Code": "SKU", "Unit cost": "RB price"})
            )

            df_national = df_national.merge(df_rb_prices, how="left", on="SKU")
            # Where we have a rollback price, use it
            df_national["Price"] = np.where(
                df_national["RB price"].isna(),
                df_national["Price"],
                df_national["RB price"],
            )
            df_national = df_national.drop(columns=["RB price"])

        # Recombine NLC + National
        df_start = pd.concat([df_nlc, df_national], ignore_index=True)
        df_start["SKU-Node"] = (
            df_start["SKU"] + "-" + df_start["Source"].fillna("").astype(str)
        )

        # Step 5: Concat all update DSVs
        valid_updates = [
            df for df in list_dsv_updates
            if df is not None and len(df) > 0
        ]
        if valid_updates:
            df_updates = pd.concat(valid_updates, ignore_index=True)
        else:
            df_updates = pd.DataFrame(columns=["SKU", "Price", "Source", "SKU-Node"])

        rows_to_update = len(df_updates)
        rows_to_add = len(df_new_nodes) if df_new_nodes is not None else 0
        final_rows = original_rows + rows_to_add

        logger.info(
            "Original: %d | Updates: %d | New nodes: %d | Expected final: %d",
            original_rows, rows_to_update, rows_to_add, final_rows,
        )

        # Remove rows that are being updated
        df_unchanged = df_start[
            ~df_start["SKU-Node"].isin(df_updates["SKU-Node"])
        ].copy()

        # Concat: unchanged + updates + new nodes
        parts = [df_unchanged, df_updates]
        if df_new_nodes is not None and len(df_new_nodes) > 0:
            parts.append(df_new_nodes)

        df_new_dsv = pd.concat(parts, ignore_index=True)

        # Drop SKU-Node helper column if present
        if "SKU-Node" in df_new_dsv.columns:
            df_new_dsv = df_new_dsv.drop(columns=["SKU-Node"])

        # Add minimum margin column
        df_new_dsv["Minimum margin"] = self.minimum_margin

        # Final column order and deduplicate
        df_new_dsv = df_new_dsv[["SKU", "Price", "Minimum margin", "Source"]]
        df_new_dsv = df_new_dsv.drop_duplicates()

        logger.info("New DSV built: %d rows", len(df_new_dsv))
        return df_new_dsv

    def save(self, df_new_dsv: pd.DataFrame, output_path: str = None) -> str:
        """Save the DSV to CSV."""
        if output_path is None:
            nlc_folder = self._settings["shared_paths"]["nlc_folder"]
            dsv_folder = os.path.join(nlc_folder, "DSV Files")
            current_month = datetime.now().strftime("%Y-%m")
            month_folder = os.path.join(dsv_folder, current_month)
            os.makedirs(month_folder, exist_ok=True)

            filename = self._config["dsv"]["output_filename_template"].format(
                date_str=self.today_str
            )
            output_path = os.path.join(month_folder, filename)

        df_new_dsv.to_csv(output_path, index=False)
        logger.info("DSV saved to: %s", output_path)
        return output_path

    def validate(self, df_new_dsv: pd.DataFrame) -> pd.DataFrame:
        """Validate the new DSV against the original.

        Computes per-row price changes and categorizes as Increase/Decrease/New/No change.

        Returns:
            DataFrame with change analysis
        """
        df_new = df_new_dsv.copy()
        df_new["Source"] = df_new["Source"].fillna("National")
        df_new["SKU-Node"] = df_new["SKU"] + "-" + df_new["Source"]

        df_old = self.df_curr_dsv_original.copy()
        df_old["source"] = df_old["source"].fillna("National")
        df_old["SKU-Node"] = df_old["sku"] + "-" + df_old["source"]

        df_check = df_new.merge(
            df_old[["SKU-Node", "walmart_dsv_price"]],
            how="left",
            on="SKU-Node",
        )
        df_check["Price change"] = round(
            df_check["Price"] - df_check["walmart_dsv_price"], 2
        )
        df_check["Price change category"] = np.where(
            df_check["Price change"] > 0,
            "Increase",
            np.where(
                df_check["Price change"] < 0,
                "Decrease",
                np.where(
                    df_check["walmart_dsv_price"].isna(),
                    "New",
                    "No change",
                ),
            ),
        )

        # Log summary
        changes = df_check[df_check["Price change"] != 0]
        logger.info("DSV validation — changes: %d", len(changes))
        logger.info(
            "  %s",
            df_check["Price change category"].value_counts().to_dict(),
        )

        return df_check
