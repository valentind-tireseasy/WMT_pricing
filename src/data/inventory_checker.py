"""Inventory check — compare two inventory snapshots to detect cost shifts.

Compares today's inventory costs against yesterday's (or any two dates),
computing per-SKU-Warehouse price deltas and summarizing by vendor/brand.
This is a diagnostic step to eyeball whether inventory costs shifted
significantly before running the pricing update.
"""

import logging
import os

import numpy as np
import pandas as pd

from src.adapters.module_loader import ensure_modules_path

logger = logging.getLogger(__name__)

# Default location for the last-run state file (project root)
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LAST_RUN_FILE = os.path.join(_PROJECT_ROOT, "last_run.txt")


def get_last_run_date() -> str | None:
    """Read the last pipeline run date from last_run.txt.

    Returns:
        Date string (YYYY-MM-DD) or None if the file doesn't exist.
    """
    if os.path.isfile(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, "r") as f:
            date_str = f.read().strip()
        if date_str:
            logger.info("Last run date from %s: %s", LAST_RUN_FILE, date_str)
            return date_str
    return None


def save_last_run_date(date_str: str):
    """Write the current run date to last_run.txt."""
    with open(LAST_RUN_FILE, "w") as f:
        f.write(date_str)
    logger.info("Saved last run date to %s: %s", LAST_RUN_FILE, date_str)


class InventoryChecker:
    """Compare two inventory snapshots and produce delta summaries.

    Args:
        date_current: Date string (YYYY-MM-DD) for the current snapshot.
        date_previous: Date string (YYYY-MM-DD) for the previous snapshot.
            Defaults to the last pipeline run date from last_run.txt,
            or the day before date_current if no state file exists.
    """

    def __init__(self, date_current: str, date_previous: str = None):
        self.date_current = date_current
        if date_previous is None:
            last_run = get_last_run_date()
            if last_run and last_run != date_current:
                self.date_previous = last_run
                logger.info(
                    "Using last run date as previous: %s", self.date_previous
                )
            else:
                prev = pd.to_datetime(date_current) - pd.Timedelta(days=1)
                self.date_previous = prev.strftime("%Y-%m-%d")
                logger.info(
                    "No last run date found, falling back to previous day: %s",
                    self.date_previous,
                )
        else:
            self.date_previous = date_previous

        self.df_inv_comp = None
        self.df_summary = None
        self.df_vendor_detail = None

    def run(self, min_lines: int = 1000) -> dict:
        """Run the full inventory check.

        Args:
            min_lines: Minimum total SKU-Warehouse lines for a vendor/brand
                to appear in the detail breakdown. Default 1000.

        Returns:
            dict with keys: df_inv_comp, df_summary, and 4 breakdown dfs:
            df_vendor_increases, df_vendor_decreases,
            df_brand_increases, df_brand_decreases
        """
        df_current, df_previous = self._load_snapshots()
        df_vendor = self._load_vendor_codes()
        self.df_inv_comp = self._compare(df_current, df_previous, df_vendor)
        self.df_summary = self._summarize(self.df_inv_comp)

        # 4 breakdowns: vendor/brand × increases/decreases
        self.df_vendor_increases = self._vendor_breakdown(
            self.df_inv_comp, category="Increase", group_col="vendor_code",
            min_lines=min_lines,
        )
        self.df_vendor_decreases = self._vendor_breakdown(
            self.df_inv_comp, category="Decrease", group_col="vendor_code",
            min_lines=min_lines,
        )
        self.df_brand_increases = self._vendor_breakdown(
            self.df_inv_comp, category="Increase", group_col="Brand code",
            min_lines=min_lines,
        )
        self.df_brand_decreases = self._vendor_breakdown(
            self.df_inv_comp, category="Decrease", group_col="Brand code",
            min_lines=min_lines,
        )

        # Keep legacy key for backward compat
        self.df_vendor_detail = self.df_vendor_increases

        logger.info(
            "Inventory check complete: %d SKU-Warehouse pairs compared",
            len(self.df_inv_comp),
        )
        return {
            "df_inv_comp": self.df_inv_comp,
            "df_summary": self.df_summary,
            "df_vendor_detail": self.df_vendor_detail,
            "df_vendor_increases": self.df_vendor_increases,
            "df_vendor_decreases": self.df_vendor_decreases,
            "df_brand_increases": self.df_brand_increases,
            "df_brand_decreases": self.df_brand_decreases,
            "date_previous": self.date_previous,
        }

    def _load_snapshots(self):
        """Load inventory snapshots for the two dates."""
        ensure_modules_path()
        import pricing_module as pricing

        logger.info(
            "Loading inventory snapshots: %s (current) vs %s (previous)",
            self.date_current,
            self.date_previous,
        )

        df_current = pricing.get_inventory(
            self.date_current, add_rebates=False, amazon=False, greater3=True
        )
        logger.info("Current inventory (%s): %d rows", self.date_current, len(df_current))

        df_previous = pricing.get_inventory(
            self.date_previous, add_rebates=False, amazon=False, greater3=True
        )
        logger.info("Previous inventory (%s): %d rows", self.date_previous, len(df_previous))

        return df_current, df_previous

    def _load_vendor_codes(self) -> pd.DataFrame:
        """Load vendor code mapping from the DW."""
        ensure_modules_path()
        import DW_connection as dw

        df_vendor = dw.get_vendor_code_table()
        df_vendor = df_vendor.rename(columns={"warehouse_code": "Warehouse Code"})
        return df_vendor[["Warehouse Code", "vendor_code"]].copy()

    def _compare(
        self,
        df_current: pd.DataFrame,
        df_previous: pd.DataFrame,
        df_vendor: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge the two snapshots and compute deltas."""
        df_comp = df_current.merge(
            df_previous,
            how="outer",
            on=["Product Code", "Warehouse Code"],
            suffixes=("_current", "_prev"),
        ).merge(df_vendor, how="left", on="Warehouse Code")

        price_curr = df_comp["Purchase Price+FET_current"]
        price_prev = df_comp["Purchase Price+FET_prev"]

        df_comp["Delta price%"] = ((price_curr - price_prev) / price_prev).round(4)

        df_comp["Delta price category"] = np.select(
            [
                df_comp["Delta price%"] < 0,
                df_comp["Delta price%"] > 0,
                df_comp["Delta price%"] == 0,
                price_prev.isna(),
                price_curr.isna(),
            ],
            [
                "Decrease",
                "Increase",
                "No change",
                "Only in current file",
                "Only in prev file",
            ],
            default="Unknown",
        )

        df_comp = df_comp[
            [
                "Product Code",
                "Warehouse Code",
                "Purchase Price+FET_current",
                "Purchase Price+FET_prev",
                "Delta price%",
                "Delta price category",
                "Available_current",
                "Available_prev",
                "vendor_code",
            ]
        ].copy()

        df_comp["Brand code"] = df_comp["Product Code"].str[:4]

        return df_comp

    def _summarize(self, df_comp: pd.DataFrame) -> pd.DataFrame:
        """Aggregate counts and average delta by category."""
        df_summary = (
            df_comp.groupby("Delta price category")
            .agg(
                count_sku_whs=("Product Code", "count"),
                avg_price_change_pct=("Delta price%", "mean"),
            )
            .reset_index()
        )
        df_summary["avg_price_change_pct"] = df_summary["avg_price_change_pct"].round(4)
        df_summary = df_summary.rename(
            columns={
                "count_sku_whs": "Count SKU-Whs",
                "avg_price_change_pct": "Avg price change %",
            }
        )
        logger.info("Inventory comparison summary:\n%s", df_summary.to_string(index=False))
        return df_summary

    def _vendor_breakdown(
        self,
        df_comp: pd.DataFrame,
        category: str = "Increase",
        group_col: str = "vendor_code",
        min_lines: int = 1000,
    ) -> pd.DataFrame:
        """Break down a delta category by vendor (or brand).

        Args:
            category: Delta category to drill into (default "Increase").
            group_col: Column to group by ("vendor_code" or "Brand code").
            min_lines: Only include groups with at least this many total lines.
        """
        df_totals = (
            df_comp.groupby(group_col)
            .agg(total_lines=("Product Code", "count"))
            .reset_index()
            .rename(columns={"total_lines": "Total wh-sku lines"})
        )

        df_cat = df_comp[df_comp["Delta price category"] == category].copy()

        df_detail = (
            df_cat.groupby(group_col)
            .agg(
                count=("Product Code", "count"),
                avg_delta=("Delta price%", "mean"),
            )
            .reset_index()
            .rename(
                columns={
                    "count": f"Count of wh-sku price {category}",
                    "avg_delta": f"Avg price {category} %",
                }
            )
        )
        df_detail[f"Avg price {category} %"] = df_detail[f"Avg price {category} %"].round(3)

        df_detail = df_detail.merge(df_totals, how="left", on=group_col)
        df_detail[f"% Lines {category}"] = (
            df_detail[f"Count of wh-sku price {category}"]
            / df_detail["Total wh-sku lines"]
        ).round(3)

        df_detail = df_detail.sort_values(
            f"Count of wh-sku price {category}", ascending=False
        )

        if min_lines > 0:
            df_detail = df_detail[df_detail["Total wh-sku lines"] >= min_lines]

        logger.info(
            "Vendor breakdown (%s, min_lines=%d): %d groups",
            category,
            min_lines,
            len(df_detail),
        )
        return df_detail
