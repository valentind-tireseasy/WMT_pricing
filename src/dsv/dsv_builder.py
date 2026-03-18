"""DSV file builder for Walmart B2B.

Takes the current DSV file, applies price updates (from rules engine),
adds new SKU-Nodes, and generates the final DSV CSV.

Optional steps (toggled via flags):
- Rollback handling: remove rollback SKUs from NLC + apply RB prices to national
- National price updates: override national prices from external Excel file
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
        builder = DSVBuilder(df_curr_dsv_original, today_str)
        df_new_dsv = builder.build(
            list_dsv_updates=[...],
            df_new_nodes=df_new_nodes_dsv,
        )
        builder.save(df_new_dsv)
    """

    def __init__(self, df_curr_dsv_original: pd.DataFrame, today_str: str = None):
        self._config = load_yaml("nlc_model.yaml")
        self._settings = load_yaml("settings.yaml")
        self.df_curr_dsv_original = df_curr_dsv_original
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")
        self.minimum_margin = self._config["dsv"]["minimum_margin"]

    def _prepare_starting_dsv(self) -> pd.DataFrame:
        """Prepare the base DSV with standardized column names and SKU-Node key."""
        df = self.df_curr_dsv_original.copy()
        df = df.rename(columns={
            "sku": "SKU",
            "walmart_dsv_price": "Price",
            "source": "Source",
        })
        df["SKU-Node"] = df["SKU"] + "-" + df["Source"].fillna("").astype(str)
        return df

    def apply_rollbacks(self, df_start: pd.DataFrame,
                        df_rollbacks: pd.DataFrame) -> pd.DataFrame:
        """Remove rollback SKUs from NLC rows and apply RB prices to national.

        This is an optional step — corresponds to '## RBs updates (no)' in the
        original notebook.

        Args:
            df_start: DSV DataFrame with SKU, Price, Source, SKU-Node columns
            df_rollbacks: Active rollbacks DataFrame (End date > today)

        Returns:
            Modified DSV DataFrame with rollbacks applied
        """
        if df_rollbacks is None or len(df_rollbacks) == 0:
            logger.info("No active rollbacks — skipping rollback step.")
            return df_start

        rollback_skus = df_rollbacks["Product Code"].unique()

        # Split NLC vs National
        df_nlc = df_start[df_start["Source"].notna()].copy()
        df_national = df_start[df_start["Source"].isna()].copy()

        # Remove rollback SKUs from NLC rows
        before = len(df_nlc)
        df_nlc = df_nlc[~df_nlc["SKU"].isin(rollback_skus)].copy()
        logger.info(
            "Rollbacks: removed %d NLC rows for %d rollback SKUs",
            before - len(df_nlc), len(rollback_skus),
        )

        # Apply rollback prices to national rows
        df_rb_prices = (
            df_rollbacks.groupby("Product Code")
            .agg({"Unit cost": "min"})
            .reset_index()
            .rename(columns={"Product Code": "SKU", "Unit cost": "RB price"})
        )

        df_national = df_national.merge(df_rb_prices, how="left", on="SKU")
        df_national["Price"] = np.where(
            df_national["RB price"].isna(),
            df_national["Price"],
            df_national["RB price"],
        )
        df_national = df_national.drop(columns=["RB price"])

        # Recombine
        df_out = pd.concat([df_nlc, df_national], ignore_index=True)
        df_out["SKU-Node"] = (
            df_out["SKU"] + "-" + df_out["Source"].fillna("").astype(str)
        )
        return df_out

    def apply_national_price_updates(self, df_start: pd.DataFrame,
                                     national_prices_path: str,
                                     sheet_name: str = "National prices",
                                     skip_rows: int = 2) -> pd.DataFrame:
        """Override national prices from an external Excel file.

        This is an optional step — corresponds to '## Update national prices (no)'
        in the original notebook.

        Args:
            df_start: DSV DataFrame with SKU, Price, Source, SKU-Node columns
            national_prices_path: Path to the Excel file with new national prices
            sheet_name: Sheet name to read from
            skip_rows: Number of header rows to skip

        Returns:
            Modified DSV DataFrame with national prices updated
        """
        df_new_prices = pd.read_excel(
            national_prices_path,
            sheet_name=sheet_name,
            dtype={"SKU": str, "Product Code": str},
            skiprows=skip_rows,
        )
        # Normalize column names
        if "Product Code" in df_new_prices.columns and "SKU" not in df_new_prices.columns:
            df_new_prices = df_new_prices.rename(columns={"Product Code": "SKU"})
        if "Min of Unit cost 3.5" in df_new_prices.columns:
            df_new_prices = df_new_prices.rename(
                columns={"Min of Unit cost 3.5": "New Price"}
            )

        # Split NLC vs National
        df_nlc = df_start[df_start["Source"].notna()].copy()
        df_national = df_start[df_start["Source"].isna()].copy()

        # Merge new prices
        df_national = df_national.merge(
            df_new_prices[["SKU", "New Price"]], how="left", on="SKU"
        )
        df_national["Price"] = np.where(
            df_national["New Price"].isna(),
            df_national["Price"],
            df_national["New Price"],
        )
        df_national = df_national.drop(columns=["New Price"])

        n_updated = df_national["Price"].notna().sum()
        logger.info("National prices: %d SKUs updated from %s", n_updated,
                     national_prices_path)

        # Recombine
        df_out = pd.concat([df_nlc, df_national], ignore_index=True)
        df_out["SKU-Node"] = (
            df_out["SKU"] + "-" + df_out["Source"].fillna("").astype(str)
        )
        return df_out

    def build(self, list_dsv_updates: list,
              df_new_nodes: pd.DataFrame) -> pd.DataFrame:
        """Build the new DSV by applying all NLC updates + new nodes.

        Call apply_rollbacks() and/or apply_national_price_updates() on the
        starting DSV BEFORE calling build() to include those optional steps.

        Args:
            list_dsv_updates: List of DSV-format DataFrames
                (each has SKU, Price, Source, SKU-Node)
            df_new_nodes: New SKU-Node DSV DataFrame

        Returns:
            Final DSV DataFrame with columns: SKU, Price, Minimum margin, Source
        """
        return self._build_from(
            self._prepare_starting_dsv(), list_dsv_updates, df_new_nodes
        )

    def build_from(self, df_start: pd.DataFrame, list_dsv_updates: list,
                   df_new_nodes: pd.DataFrame) -> pd.DataFrame:
        """Build the new DSV from a pre-processed starting DSV.

        Use this when you've already applied optional steps (rollbacks,
        national price updates) to df_start.
        """
        return self._build_from(df_start, list_dsv_updates, df_new_nodes)

    def _build_from(self, df_start: pd.DataFrame, list_dsv_updates: list,
                    df_new_nodes: pd.DataFrame) -> pd.DataFrame:
        """Core DSV build logic."""
        original_rows = len(df_start)
        logger.info("Starting DSV: %d rows", original_rows)

        # Concat all update DSVs
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

        logger.info(
            "Original: %d | Updates: %d | New nodes: %d | Expected final: %d",
            original_rows, rows_to_update, rows_to_add,
            original_rows + rows_to_add,
        )

        # Remove rows being updated
        df_unchanged = df_start[
            ~df_start["SKU-Node"].isin(df_updates["SKU-Node"])
        ].copy()

        # Concat: unchanged + updates + new nodes
        parts = [df_unchanged, df_updates]
        if df_new_nodes is not None and len(df_new_nodes) > 0:
            parts.append(df_new_nodes)

        df_new_dsv = pd.concat(parts, ignore_index=True)

        if "SKU-Node" in df_new_dsv.columns:
            df_new_dsv = df_new_dsv.drop(columns=["SKU-Node"])

        df_new_dsv["Minimum margin"] = self.minimum_margin
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
        """Validate the new DSV against the original."""
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

        changes = df_check[df_check["Price change"] != 0]
        logger.info("DSV validation — changes: %d", len(changes))
        logger.info(
            "  %s",
            df_check["Price change category"].value_counts().to_dict(),
        )

        return df_check
