"""DSV file builder for Walmart B2B.

Takes the current DSV file, applies price updates (from rules engine),
adds new SKU-Nodes, removes rollback SKUs, and generates the final
DSV CSV file for upload via hybris.
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
        builder = DSVBuilder(
            df_current_dsv=df_current_dsv,
            df_updates=df_updates,
            df_new_nodes=df_new_nodes,
            df_rollbacks=df_rollbacks,
            today_str="2026-03-18",
        )
        df_new_dsv = builder.build()
        builder.save(df_new_dsv)
    """

    def __init__(self, df_current_dsv: pd.DataFrame, df_updates: pd.DataFrame,
                 df_new_nodes: pd.DataFrame, df_rollbacks: pd.DataFrame = None,
                 today_str: str = None):
        self._config = load_yaml("nlc_model.yaml")
        self._settings = load_yaml("settings.yaml")
        self.df_current_dsv = df_current_dsv
        self.df_updates = df_updates
        self.df_new_nodes = df_new_nodes
        self.df_rollbacks = df_rollbacks
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")
        self.minimum_margin = self._config["dsv"]["minimum_margin"]

    def build(self) -> pd.DataFrame:
        """Build the new DSV by applying all updates to the current DSV.

        Returns:
            DataFrame with columns: SKU, Price, Minimum margin, Source
        """
        original_rows = len(self.df_current_dsv)
        logger.info("Building new DSV from %d original rows", original_rows)

        # Prepare current DSV with SKU-Node key
        df_start = self.df_current_dsv.copy()
        if "sku" in df_start.columns and "SKU" not in df_start.columns:
            df_start = df_start.rename(columns={
                "sku": "SKU",
                "walmart_dsv_price": "Price",
                "source": "Source",
            })
        df_start["SKU-Node"] = (
            df_start["SKU"] + "-" + df_start["Source"].fillna("").astype(str)
        )

        # Handle rollbacks: remove rollback SKUs from NLC nodes (keep national)
        if self.df_rollbacks is not None and len(self.df_rollbacks) > 0:
            rollback_skus = self.df_rollbacks["Product Code"].unique()
            df_nlc = df_start[df_start["Source"].notna()].copy()
            df_national = df_start[df_start["Source"].isna()].copy()

            df_nlc_no_rbs = df_nlc[~df_nlc["SKU"].isin(rollback_skus)]
            df_start = pd.concat([df_nlc_no_rbs, df_national], ignore_index=True)
            df_start["SKU-Node"] = (
                df_start["SKU"] + "-" + df_start["Source"].fillna("").astype(str)
            )
            logger.info("Removed %d rollback SKU rows from NLC nodes",
                        original_rows - len(df_start))

        # Remove rows that will be updated
        update_nodes = set()
        if self.df_updates is not None and len(self.df_updates) > 0:
            update_nodes = set(self.df_updates["SKU-Node"].unique())

        df_unchanged = df_start[~df_start["SKU-Node"].isin(update_nodes)].copy()

        # Concat: unchanged + updates + new nodes
        parts = [df_unchanged]

        if self.df_updates is not None and len(self.df_updates) > 0:
            parts.append(self.df_updates[["SKU", "Price", "Source"]])

        if self.df_new_nodes is not None and len(self.df_new_nodes) > 0:
            parts.append(self.df_new_nodes[["SKU", "Price", "Source"]])

        df_new_dsv = pd.concat(parts, ignore_index=True)

        # Add minimum margin column
        df_new_dsv["Minimum margin"] = self.minimum_margin

        # Final columns
        df_new_dsv = df_new_dsv[["SKU", "Price", "Minimum margin", "Source"]]

        logger.info(
            "New DSV built: %d rows (was %d, updates=%d, new=%d)",
            len(df_new_dsv),
            original_rows,
            len(self.df_updates) if self.df_updates is not None else 0,
            len(self.df_new_nodes) if self.df_new_nodes is not None else 0,
        )

        return df_new_dsv

    def save(self, df_new_dsv: pd.DataFrame, output_path: str = None) -> str:
        """Save the DSV to CSV.

        Args:
            df_new_dsv: The built DSV DataFrame.
            output_path: Override output path. Defaults to shared drive location.

        Returns:
            The path the file was saved to.
        """
        if output_path is None:
            nlc_folder = self._settings["shared_paths"]["nlc_folder"]
            dsv_folder = os.path.join(nlc_folder, "DSV Files")
            current_month = datetime.now().strftime("%Y-%m")
            month_folder = os.path.join(dsv_folder, current_month)
            os.makedirs(month_folder, exist_ok=True)
            output_path = os.path.join(month_folder, f"DSV {self.today_str}.csv")

        df_new_dsv.to_csv(output_path, index=False)
        logger.info("DSV saved to: %s", output_path)
        return output_path

    def validate(self, df_new_dsv: pd.DataFrame) -> dict:
        """Run validation checks on the new DSV.

        Returns:
            dict with check results (pass/fail + details)
        """
        checks = {}

        # Check 1: No duplicate SKU-Nodes
        df_check = df_new_dsv.copy()
        df_check["Source"] = df_check["Source"].fillna("National")
        df_check["SKU-Node"] = df_check["SKU"] + "-" + df_check["Source"]
        dups = df_check["SKU-Node"].duplicated().sum()
        checks["no_duplicate_sku_nodes"] = {
            "pass": dups == 0,
            "detail": f"{dups} duplicates found",
        }

        # Check 2: No negative prices
        neg = (df_new_dsv["Price"] <= 0).sum()
        checks["no_negative_prices"] = {
            "pass": neg == 0,
            "detail": f"{neg} rows with price <= 0",
        }

        # Check 3: Row count within expected range
        original = len(self.df_current_dsv)
        new = len(df_new_dsv)
        pct_change = abs(new - original) / original if original > 0 else 0
        checks["row_count_reasonable"] = {
            "pass": pct_change < 0.10,  # Less than 10% change
            "detail": f"Original={original}, New={new}, Change={pct_change:.1%}",
        }

        all_pass = all(c["pass"] for c in checks.values())
        logger.info("DSV validation: %s", "PASS" if all_pass else "FAIL")
        for name, result in checks.items():
            status = "PASS" if result["pass"] else "FAIL"
            logger.info("  %s: %s — %s", name, status, result["detail"])

        return checks
