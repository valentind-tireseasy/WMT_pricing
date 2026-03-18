"""Node Level Cost pricing model.

Computes optimal node-level costs per SKU-Node based on inventory coverage,
margin targets, MAP constraints, and Walmart margin constraints.

Replicates the logic from the original notebook's calculate_new_node_level_cost()
function and the two-pass inventory strategy (min_units=8, then min_units=4).
"""

import logging

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml, ensure_modules_path

logger = logging.getLogger(__name__)


def process_inventory_nlc(df_inventory, df_wh_node_mapping, df_curr_dsv_skus,
                          metric_group="min", min_units=4):
    """Process raw inventory into per-SKU-Node aggregated stats.

    Matches the notebook's process_inventory_nlc() function exactly:
    1. Filter by Available >= min_units
    2. Filter to SKUs that exist in the current national DSV
    3. Merge with warehouse node mapping
    4. Filter by Available >= Zero Out Threshold
    5. Filter to rows with a Node type (mapped nodes only)
    6. Group by Product Code + Identifier + date → take min Purchase Price+FET
    7. Take most recent date per Product Code + Identifier

    Args:
        df_inventory: Multi-date inventory DataFrame (with "date" column)
        df_wh_node_mapping: Filtered warehouse node mapping
        df_curr_dsv_skus: Series/list of SKUs in the current national DSV
        metric_group: Aggregation method ("min" for min cost)
        min_units: Minimum Available units threshold

    Returns:
        DataFrame with one row per Product Code + Identifier (most recent date)
    """
    df = df_inventory.copy()
    df["Brand code"] = df["Product Code"].str[:4]

    # Step 1: Filter by min units
    df = df[df["Available"] >= min_units].copy()

    # Step 2: Filter to SKUs in current DSV (national prices)
    df = df[df["Product Code"].isin(df_curr_dsv_skus)].copy()

    # Step 3: Merge with node mapping
    df = df.merge(df_wh_node_mapping, how="left", on="Warehouse Code")

    # Step 4: Filter by Zero Out Threshold
    df = df[df["Available"] >= df["Zero Out Threshold"]].copy()

    # Step 5: Only mapped nodes
    df = df[df["Node type"].notna()].copy()

    # Step 6: Per SKU-Node-date, take min Purchase Price+FET
    df_min_prices = df.groupby(
        ["Product Code", "Identifier", "date"]
    ).agg({"Purchase Price+FET": metric_group}).reset_index()

    # Inner join back to get the full row with min price
    df = df.merge(
        df_min_prices,
        how="inner",
        on=["Product Code", "Identifier", "Purchase Price+FET", "date"],
    )
    df = df.drop_duplicates(
        subset=["Product Code", "Identifier", "Purchase Price+FET", "date"],
        keep="first",
    )
    df = df[
        ["Product Code", "Identifier", "Warehouse Code", "Available",
         "Purchase Price+FET", "date", "Node type"]
    ].copy()

    # Step 7: Most recent date per SKU-Node
    df = (
        df.sort_values("date", ascending=False)
        .groupby(["Product Code", "Identifier"], as_index=False)
        .first()
    )

    return df


class NLCModel:
    """Compute node-level costs for Walmart B2B pricing.

    Implements the full notebook pipeline:
    - Two-pass inventory (min_units=8, then min_units=4 for gaps)
    - NLC computation at 8 margin levels
    - MAP and Walmart margin constraints
    - Final NLC cascade: 11% -> 8% -> 6% -> N/A
    - Margin split calculations for tests
    - Sales revenue categorization

    Usage:
        model = NLCModel(date_str="2026-03-18")
        model.load_data(loader)
        df_output = model.run()
    """

    def __init__(self, date_str: str):
        self._config = load_yaml("nlc_model.yaml")
        self._rules_config = load_yaml("pricing_rules.yaml")
        self.date_str = date_str
        self.margins = self._config["margins"]
        self.final_cascade = self._config["final_nlc_cascade"]
        self.distance_map = self._config["distance_map"]
        self.min_wm_margin = self._config["min_wm_margin"]
        self.margin_splits = self._config["margin_splits"]
        self.min_units_primary = self._config["inventory"]["min_units_primary"]
        self.min_units_secondary = self._config["inventory"]["min_units_secondary"]
        self.metric_group = self._config["inventory"]["metric_group"]
        self.dont_update_targets = self._config["dont_update_targets"]
        self.min_margin_update = self._config["min_margin_update_prices"]
        self.max_margin_update = self._config["max_margin_update_prices"]

        # Data attributes — populated by load_data()
        self.df_inv_all = None
        self.df_avg_wmt_price = None
        self.df_map = None
        self.df_wh_node_mapping = None
        self.df_cost_node = None
        self.df_rollbacks = None
        self.df_curr_dsv_original = None
        self.df_curr_dsv_nlc = None
        self.df_curr_dsv = None
        self.df_current_tests = None
        self.df_sales_sku_node = None
        self.df_sku_revenue = None
        self.df_sku_node_revenue = None

    def load_data(self, loader, rollbacks_path: str = None):
        """Load all required data sources via the DataLoader.

        Args:
            loader: DataLoader instance.
            rollbacks_path: Full path to the approved rollbacks Excel file.
                The file changes monthly (e.g. ".../2026-01 RBs/Approved RBs/
                Approved RBs start date 2026-02-01.xlsx"). If None, rollbacks
                are skipped (df_rollbacks will be an empty DataFrame).
        """
        from dateutil.relativedelta import relativedelta

        date = pd.to_datetime(self.date_str)

        logger.info("Loading NLC model data for date=%s", self.date_str)

        # --- DSV (current) ---
        self.df_curr_dsv_original = loader.load_dsv_by_date()
        self._split_dsv()

        # --- Warehouse node mapping ---
        df_wh_all = loader.load("warehouse_node_mapping")
        # Filter to WalmartB2B, enabled, inventory enabled
        df_wh_all = df_wh_all[df_wh_all["Channel"] == "WalmartB2B"]
        self.df_wh_node_mapping = df_wh_all[
            (df_wh_all["Warehouse Status"] == "ENABLED")
            & (df_wh_all["Identifier Status"] == "ENABLED")
            & (df_wh_all["Inventory Enabled"] == 1)
        ].copy()
        self.df_wh_node_mapping = self.df_wh_node_mapping.rename(
            columns={"Type": "Node type", "Inventory Threshold": "Zero Out Threshold"}
        )

        # --- Walmart Item Report → average prices ---
        df_wmt = loader.load("dw_walmart_item_report", date_str=self.date_str)
        self.df_avg_wmt_price = (
            df_wmt.groupby("Product Code")
            .agg({"offer_price": "mean", "unit_cost": "mean"})
            .reset_index()
        )

        # --- MAP ---
        self.df_map = loader.load("dw_map_prices", date_str=self.date_str)

        # --- Shipping costs ---
        self.df_cost_node = loader.load("shipping_costs_by_node")

        # --- Rollbacks ---
        if rollbacks_path:
            df_rollbacks_all = loader.load(
                "rollbacks", rollbacks_path=rollbacks_path
            )
            # Filter to active rollbacks (End date > today)
            self.df_rollbacks = df_rollbacks_all[
                df_rollbacks_all["End date"] > pd.to_datetime("today")
            ].copy()
            logger.info(
                "Rollbacks loaded: %d active (from %d total)",
                len(self.df_rollbacks),
                len(df_rollbacks_all),
            )
        else:
            self.df_rollbacks = pd.DataFrame(columns=["Product Code", "End date", "Unit cost"])
            logger.info("No rollbacks path provided — skipping rollbacks.")

        # --- Sales ---
        days_sales = self._config["days_sales"]
        start_date = (date - relativedelta(days=days_sales)).strftime("%Y-%m-%d")
        self.df_sales_sku_node = loader.load(
            "dw_walmart_sales", start_date=start_date
        )
        self._process_sales()

        # --- Inventory (multi-date via shared pricing module) ---
        self.df_inv_all = self._load_inventory(date)

        # --- Tests tracker ---
        self.df_current_tests = loader.load("tests_tracker")
        self.df_current_tests["SKU-Node"] = (
            self.df_current_tests["Product Code"]
            + "-"
            + self.df_current_tests["Identifier"].astype(str)
        )

        logger.info("All NLC model data loaded.")

    def _split_dsv(self):
        """Split DSV into NLC (has Source) and National (no Source) parts."""
        df = self.df_curr_dsv_original.copy()

        # NLC rows (have a Source/Identifier)
        df_nlc = df[df["source"].notna()].copy()
        self.df_curr_dsv_nlc = df_nlc.rename(columns={
            "sku": "Product Code",
            "source": "Identifier",
            "walmart_dsv_price": "current_nlc_price",
        })

        # National rows (Source is NaN)
        self.df_curr_dsv = df[df["source"].isna()].copy()
        self.df_curr_dsv = self.df_curr_dsv.rename(columns={"sku": "SKU"})

    def _process_sales(self):
        """Process sales data into SKU and SKU-Node revenue aggregates."""
        df = self.df_sales_sku_node.copy()
        df["SKU-Node"] = df["sku"] + "-" + df["externalwarehouseid"].astype(str)
        df["order_date"] = pd.to_datetime(df["order_date"])

        # SKU-level revenue
        df_sales = (
            df.groupby(["order_date", "sku"])
            .agg({"quantity": "sum", "total_inv_amount": "sum", "profit": "sum"})
            .reset_index()
            .rename(columns={"total_inv_amount": "revenue", "sku": "Product Code"})
        )

        df_sku_rev = (
            df_sales.groupby("Product Code")
            .agg({"revenue": "sum"})
            .reset_index()
            .rename(columns={"revenue": "sku_revenue"})
        )

        # Sales category (revenue percentile buckets)
        df_sales_filt = (
            df_sales.groupby("Product Code")
            .agg({"revenue": "sum"})
            .reset_index()
            .sort_values("revenue", ascending=False)
            .reset_index(drop=True)
        )
        df_sales_filt["cumulative_revenue"] = df_sales_filt["revenue"].cumsum()
        total_rev = df_sales_filt["revenue"].sum()
        df_sales_filt["cumulative_revenue_pct"] = (
            df_sales_filt["cumulative_revenue"] / total_rev
        )

        bins_cfg = self._rules_config["sku_sales_bins"]
        df_sales_filt["sku_sales_category"] = pd.cut(
            df_sales_filt["cumulative_revenue_pct"],
            bins=bins_cfg["edges"],
            labels=bins_cfg["labels"],
        )

        self.df_sku_revenue = df_sku_rev.merge(
            df_sales_filt[["Product Code", "sku_sales_category"]],
            on="Product Code",
            how="left",
        )

        # SKU-Node level revenue
        self.df_sku_node_revenue = (
            df.groupby("SKU-Node")
            .agg({"total_inv_amount": "sum"})
            .reset_index()
            .rename(columns={"total_inv_amount": "sku_node_revenue"})
        )

        total_revenue = self.df_sku_node_revenue["sku_node_revenue"].sum()
        logger.info(
            "Total SKU-Node revenue (last %d days): %.0f",
            self._config["days_sales"],
            total_revenue,
        )

    def _load_inventory(self, date):
        """Load inventory for the lookback window using the shared pricing module."""
        ensure_modules_path()
        import pricing_module as pricing

        days_before = self._config["days_before"]
        dates = (
            pd.date_range(end=date, periods=days_before, freq="D")
            .strftime("%Y-%m-%d")
            .tolist()
        )

        df_all = pd.DataFrame()
        for date_i in dates:
            df_date = pricing.get_inventory(
                date_i, add_rebates=False, amazon=False, greater3=True
            )
            df_date["date"] = pd.to_datetime(date_i)
            logger.info("Inventory loaded for %s: %d rows", date_i, len(df_date))
            df_all = pd.concat([df_all, df_date], ignore_index=True)

        # Exclude rollback SKUs from inventory
        rollback_skus = self.df_rollbacks["Product Code"].unique()
        df_all = df_all[~df_all["Product Code"].isin(rollback_skus)]

        logger.info("Total inventory rows (excl rollbacks): %d", len(df_all))
        return df_all

    def run(self) -> pd.DataFrame:
        """Run the NLC model and return the output DataFrame.

        Two-pass strategy:
        1. Run with min_units=8 (primary)
        2. Run with min_units=4 (secondary)
        3. Merge: use min_units=8 where available, fill gaps with min_units=4
        4. Merge with tests tracker for existing assignments

        Returns:
            DataFrame with all SKU-Node rows and computed prices.
        """
        logger.info("Running NLC model...")

        # Pass 1: min_units=8
        df_all_8 = self._calculate_nlc(min_units=self.min_units_primary)
        df_8 = df_all_8[df_all_8["Final node level cost category"] != "N/A"].copy()
        logger.info("Pass 1 (min_units=%d): %d workable rows",
                     self.min_units_primary, len(df_8))

        # Pass 2: min_units=4
        df_all_4 = self._calculate_nlc(min_units=self.min_units_secondary)
        df_4 = df_all_4[df_all_4["Final node level cost category"] != "N/A"].copy()
        logger.info("Pass 2 (min_units=%d): %d workable rows",
                     self.min_units_secondary, len(df_4))

        # Merge: prefer min_units=8, fill with min_units=4
        df_4_not_in_8 = df_4[~df_4["SKU-Node"].isin(df_8["SKU-Node"])].copy()
        df_output = pd.concat([df_8, df_4_not_in_8], ignore_index=True)
        logger.info("Combined output: %d rows", len(df_output))

        # Merge with tests tracker for existing assignments
        tracker_cols = ["SKU-Node", "Final target", "Start date", "Sub-group"]
        tracker_available = [
            c for c in tracker_cols if c in self.df_current_tests.columns
        ]
        df_output = df_output.merge(
            self.df_current_tests[tracker_available],
            how="left",
            on="SKU-Node",
        )

        logger.info("NLC model complete: %d SKU-Node rows", len(df_output))
        return df_output

    def _calculate_nlc(self, min_units: int) -> pd.DataFrame:
        """Core NLC computation for a given min_units threshold.

        Replicates calculate_new_node_level_cost() from the notebook exactly.
        """
        # Process inventory
        df_inv = process_inventory_nlc(
            self.df_inv_all,
            self.df_wh_node_mapping,
            self.df_curr_dsv["SKU"],
            metric_group=self.metric_group,
            min_units=min_units,
        )

        # Merge: inventory + walmart prices + MAP + shipping costs
        df = (
            df_inv.merge(self.df_avg_wmt_price, how="left", on="Product Code")
            .merge(self.df_map, how="left", on="Product Code")
            .merge(self.df_cost_node, how="left", on="Identifier")
        )

        # Shipping cost defaults to 0
        df["Shipping cost"] = df["Shipping cost"].fillna(0)
        df["Cost+Shipping"] = df["Purchase Price+FET"] + df["Shipping cost"]
        df["Is MAP now?"] = np.where(df["MAP"].isna(), "No", "Yes")

        # Compute NLC at each margin level
        for margin in self.margins:
            pct = int(margin * 100)
            suffix = f" - {pct}% margin"

            # Base NLC = cost / (1 - margin)
            df[f"Node level cost{suffix}"] = round(
                df["Purchase Price+FET"] / (1 - margin), 2
            )

            # Walmart margin at this NLC
            df[f"Walmart Margin{suffix}"] = round(
                (df["offer_price"] - df[f"Node level cost{suffix}"])
                / df["offer_price"],
                4,
            )

            # MAP constraint check
            df[f"Node cost < MAP - min margin%{suffix}"] = np.where(
                df[f"Node level cost{suffix}"] < df["MAP"] * (1 - self.distance_map),
                "Yes",
                "No",
            )
            df[f"Node cost - MAP{suffix}"] = (
                df[f"Node level cost{suffix}"] - df["MAP"]
            )

            # Target eligibility: If MAP, must be below MAP ceiling.
            # If not MAP, Walmart margin must be > min_wm_margin.
            df[f"Target for node level cost?{suffix}"] = np.where(
                df["Is MAP now?"] == "Yes",
                np.where(
                    df[f"Node cost < MAP - min margin%{suffix}"] == "No",
                    "No",
                    "Yes",
                ),
                np.where(
                    df[f"Walmart Margin{suffix}"] > self.min_wm_margin,
                    "Yes",
                    "No",
                ),
            )

            # Add shipping cost to NLC
            df[f"Node level cost{suffix}"] = (
                df[f"Node level cost{suffix}"] + df["Shipping cost"]
            )

        # Merge with current NLC prices
        df = df.merge(
            self.df_curr_dsv_nlc, how="left", on=["Product Code", "Identifier"]
        )

        # Final NLC category: cascade 11% -> 8% -> 6% -> N/A
        cascade = self.final_cascade  # [0.11, 0.08, 0.06]
        cascade_pcts = [f"{int(m * 100)}%" for m in cascade]

        df["Final node level cost category"] = "N/A"
        df["Final node level cost"] = np.nan
        df["Final walmart margin"] = np.nan

        for margin_val, pct_label in zip(reversed(cascade), reversed(cascade_pcts)):
            suffix = f" - {pct_label} margin"
            target_col = f"Target for node level cost?{suffix}"
            nlc_col = f"Node level cost{suffix}"
            wm_col = f"Walmart Margin{suffix}"

            mask = df[target_col] == "Yes"
            df.loc[mask, "Final node level cost category"] = pct_label
            df.loc[mask, "Final node level cost"] = df.loc[mask, nlc_col]
            df.loc[mask, "Final walmart margin"] = df.loc[mask, wm_col]

        # Price change vs current
        df["Final price change %"] = round(
            (df["Final node level cost"] - df["current_nlc_price"])
            / df["current_nlc_price"],
            3,
        )
        df["Final price change category"] = np.where(
            df["Final price change %"] < 0,
            "Decrease",
            np.where(df["Final price change %"] > 0, "Increase", "No change"),
        )

        # SKU-Node key
        df["SKU-Node"] = df["Product Code"] + "-" + df["Identifier"].astype(str)
        df["Brand code"] = df["Product Code"].str[:4]
        df["New NLC"] = np.where(df["current_nlc_price"].isna(), "Yes", "No")
        df["Min units"] = min_units

        # Current NLC margin (uses Cost+Shipping, not just Purchase Price+FET)
        df["current_nlc_margin"] = round(
            (df["current_nlc_price"] - df["Cost+Shipping"]) / df["current_nlc_price"],
            4,
        )

        # NLC margin categories
        margin_bins = self._rules_config["nlc_margin_bins"]
        df["current_nlc_margin category"] = pd.cut(
            df["current_nlc_margin"],
            bins=margin_bins["edges"],
            labels=margin_bins["labels"],
        )

        # Walmart margins
        df["Current walmart margin at National"] = round(
            (df["offer_price"] - df["unit_cost"]) / df["offer_price"], 4
        )
        df["Current walmart margin at NLC"] = round(
            (df["offer_price"] - df["current_nlc_price"]) / df["offer_price"], 4
        )

        wm_bins = self._rules_config["wm_margin_bins"]
        df["Current walmart margin at NLC category"] = pd.cut(
            df["Current walmart margin at NLC"],
            bins=wm_bins["edges"],
            labels=wm_bins["labels"],
        )
        df["Walmart margin at new NLC category"] = pd.cut(
            df["Final walmart margin"],
            bins=wm_bins["edges"],
            labels=wm_bins["labels"],
        )

        # Total margin = Walmart margin at NLC + our NLC margin
        df["Total margin"] = round(
            df["Current walmart margin at NLC"] + df["current_nlc_margin"], 4
        )

        # Margin split calculations (for Wm margin split test)
        for split in self.margin_splits:
            pct = int(split * 100)
            df[f"Wmt margin {pct}% group"] = round(
                df["Total margin"] * (1 - split), 4
            )
            df[f"Price margin split {pct}%"] = round(
                df["offer_price"] * (1 - df[f"Wmt margin {pct}% group"]), 2
            )
            # Cap at 20% margin NLC
            df[f"Price margin split {pct}%"] = np.minimum(
                df[f"Price margin split {pct}%"],
                df["Node level cost - 20% margin"],
            )

        # Merge revenue data
        df = df.merge(self.df_sku_node_revenue, how="left", on="SKU-Node")
        df = df.merge(self.df_sku_revenue, how="left", on="Product Code")

        total_rows = len(df)
        workable = len(df[df["Final node level cost category"] != "N/A"])
        logger.info(
            "NLC calc (min_units=%d): %d total, %d workable",
            min_units, total_rows, workable,
        )

        return df
