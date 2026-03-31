"""Correlation analysis data preparation.

Loads and assembles the master DataFrame for all downstream analysis.
Extracts cells 2-33 and 56-57 of the original notebook.
"""

import logging
import re
import time

import numpy as np
import pandas as pd

from src.data.loader import DataLoader
from src.adapters.module_loader import load_yaml, ensure_modules_path
from src.analysis.config import load_analysis_config

logger = logging.getLogger(__name__)


class AnalysisDataPrep:
    """Build the master analysis DataFrame from raw data sources.

    Usage::

        prep = AnalysisDataPrep(end_date="2026-03-25")
        df = prep.run()
        df = prep.load_extended_features(df)   # optional: tire size, MAP flag
        prep.close()
    """

    def __init__(
        self,
        end_date: str,
        *,
        loader: DataLoader = None,
        config: dict = None,
        rollbacks_path: str = None,
        warehouse_addresses_path: str = None,
    ):
        self.end_date = end_date
        self.loader = loader or DataLoader()
        self.cfg = (config or load_analysis_config())["data_prep"]
        self.nlc_config = load_yaml("nlc_model.yaml")
        self.rollbacks_path = rollbacks_path
        self.warehouse_addresses_path = warehouse_addresses_path

        # Derived dates
        self.end_dt = pd.to_datetime(end_date)
        self.analysis_days = self.cfg["analysis_days"]
        self.rolling_window = self.cfg["rolling_window"]
        self.start_dt = self.end_dt - pd.Timedelta(days=self.analysis_days - 1)
        self.warmup_dt = self.start_dt - pd.Timedelta(days=self.rolling_window)
        self.sku_filter_start_dt = self.end_dt - pd.Timedelta(
            days=self.cfg["sku_sales_lookback_days"]
        )

        # String versions for loader calls
        self.start_date = self.start_dt.strftime("%Y-%m-%d")
        self.warmup_date = self.warmup_dt.strftime("%Y-%m-%d")
        self.sku_filter_start = self.sku_filter_start_dt.strftime("%Y-%m-%d")

        # State
        self._top_skus = None
        self._df_dsv_all = None
        self._df_wh_mapping = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Execute full data preparation pipeline.

        Returns the master DataFrame (~40 columns) ready for analysis.
        """
        logger.info(
            "Analysis window: %s to %s (warmup from %s)",
            self.start_date, self.end_date, self.warmup_date,
        )

        top_skus = self._filter_top_skus()
        self._top_skus = top_skus

        df_sales_agg, sku_nodes = self._load_and_aggregate_sales(top_skus)
        scaffold = self._build_scaffold(df_sales_agg, sku_nodes)
        scaffold = self._load_dsv_history(scaffold, top_skus)
        scaffold = self._merge_offer_prices(scaffold, top_skus)
        scaffold = self._merge_supporting_data(scaffold)
        scaffold = self._load_and_merge_inventory(scaffold, top_skus)
        scaffold = self._compute_columns(scaffold)
        scaffold = self._compute_days_since_price_change(scaffold)
        scaffold = self._compute_rolling_features(scaffold)
        df = self._trim_warmup(scaffold)

        logger.info("Final dataset shape: %s", df.shape)
        return df

    def load_extended_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add tire size, tire diameter, and MAP flag columns.

        Cells 56-57 of the original notebook.
        """
        df = self._load_tire_size(df)
        df["is_MAP_tire"] = df["MAP"].notna()
        logger.info("MAP tire %%: %.1f%%", df["is_MAP_tire"].mean() * 100)
        return df

    def close(self):
        """Close the DataLoader (and its DW connection if open)."""
        if hasattr(self.loader, "close"):
            self.loader.close()

    # ------------------------------------------------------------------
    # Private: SKU filtering (cells 7-9)
    # ------------------------------------------------------------------

    def _filter_top_skus(self) -> set:
        df_sales_filter = self.loader.load(
            "dw_walmart_sales", start_date=self.sku_filter_start
        )
        logger.info(
            "Sales rows (last %d days): %s",
            self.cfg["sku_sales_lookback_days"], f"{len(df_sales_filter):,}",
        )

        df_sku_qty = (
            df_sales_filter.groupby("sku")["quantity"]
            .sum()
            .reset_index()
            .rename(columns={"quantity": "total_qty"})
            .sort_values("total_qty", ascending=False)
            .reset_index(drop=True)
        )
        df_sku_qty["cum_qty"] = df_sku_qty["total_qty"].cumsum()
        df_sku_qty["cum_pct"] = df_sku_qty["cum_qty"] / df_sku_qty["total_qty"].sum()

        cutoff_idx = (df_sku_qty["cum_pct"] <= self.cfg["top_sku_pct"]).sum()
        top_skus = set(df_sku_qty.iloc[: cutoff_idx + 1]["sku"].tolist())

        logger.info(
            "Top %.0f%% SKUs selected: %s / %s",
            self.cfg["top_sku_pct"] * 100, f"{len(top_skus):,}",
            f"{df_sku_qty.shape[0]:,}",
        )

        # Optionally exclude rollback SKUs
        if self.rollbacks_path:
            df_rollbacks = self.loader.load(
                "rollbacks", rollbacks_path=self.rollbacks_path
            )
            df_rollbacks["End date"] = pd.to_datetime(df_rollbacks["End date"])
            active_rollbacks = df_rollbacks[
                df_rollbacks["End date"] > self.start_dt
            ]["Product Code"].unique()
            before = len(top_skus)
            top_skus -= set(active_rollbacks)
            logger.info(
                "Excluded %d rollback SKUs, %d remaining",
                before - len(top_skus), len(top_skus),
            )

        return top_skus

    # ------------------------------------------------------------------
    # Private: Sales loading (cells 11-12)
    # ------------------------------------------------------------------

    def _load_and_aggregate_sales(self, top_skus):
        df_sales_raw = self.loader.load(
            "dw_walmart_sales", start_date=self.warmup_date
        )
        df_sales_raw["order_date"] = pd.to_datetime(df_sales_raw["order_date"])

        df_sales_raw = df_sales_raw[
            (df_sales_raw["sku"].isin(top_skus))
            & (df_sales_raw["order_date"] <= self.end_dt)
        ].copy()

        logger.info("Sales rows after filtering: %s", f"{len(df_sales_raw):,}")

        df_sales_agg = (
            df_sales_raw.groupby(["sku", "externalwarehouseid", "order_date"])
            .agg(
                qty_sold=("quantity", "sum"),
                revenue=("total_inv_amount", "sum"),
                profit=("profit", "sum"),
            )
            .reset_index()
            .rename(columns={"externalwarehouseid": "node", "order_date": "date"})
        )
        df_sales_agg["node"] = df_sales_agg["node"].astype(str)

        sku_nodes = df_sales_agg[["sku", "node"]].drop_duplicates()
        logger.info("Unique SKU-Nodes with sales: %s", f"{len(sku_nodes):,}")

        return df_sales_agg, sku_nodes

    # ------------------------------------------------------------------
    # Private: Scaffold (cell 14)
    # ------------------------------------------------------------------

    def _build_scaffold(self, df_sales_agg, sku_nodes):
        all_dates = pd.date_range(self.warmup_dt, self.end_dt, freq="D")
        df_dates = pd.DataFrame({"date": all_dates})

        scaffold = sku_nodes.merge(df_dates, how="cross")
        scaffold = scaffold.merge(
            df_sales_agg, on=["sku", "node", "date"], how="left"
        )
        scaffold["qty_sold"] = scaffold["qty_sold"].fillna(0)
        scaffold["revenue"] = scaffold["revenue"].fillna(0)
        scaffold["profit"] = scaffold["profit"].fillna(0)

        logger.info(
            "Scaffold: %s rows (%s SKU-Nodes x %d days)",
            f"{scaffold.shape[0]:,}", f"{len(sku_nodes):,}", len(all_dates),
        )
        return scaffold

    # ------------------------------------------------------------------
    # Private: DSV history (cells 16-18)
    # ------------------------------------------------------------------

    def _load_dsv_history(self, scaffold, top_skus):
        dsv_config = self.loader.get_source_config("walmart_dsv_folder")
        dsv_files = self.loader.google.get_folder_files(dsv_config["id"])

        dsv_files["parsed_date"] = dsv_files["Name"].apply(
            lambda x: re.search(r"\d{4}-\d{2}-\d{2}", str(x))
        )
        dsv_files = dsv_files[dsv_files["parsed_date"].notna()].copy()
        dsv_files["dsv_date"] = dsv_files["parsed_date"].apply(
            lambda m: pd.to_datetime(m.group())
        )
        dsv_files = dsv_files.drop(columns=["parsed_date"])

        buffer_start = self.warmup_dt - pd.Timedelta(days=30)
        dsv_files = dsv_files[
            (dsv_files["dsv_date"] >= buffer_start)
            & (dsv_files["dsv_date"] <= self.end_dt)
        ].sort_values("dsv_date")

        # Deduplicate: keep latest file per date
        dsv_files = (
            dsv_files.sort_values("Name").groupby("dsv_date").last().reset_index()
        )
        dsv_files = dsv_files.sort_values("dsv_date")
        logger.info("DSV files in range (deduplicated): %d", len(dsv_files))

        # Load each DSV file
        dsv_snapshots = []
        for idx, (_, row) in enumerate(dsv_files.iterrows()):
            df_dsv_i = self.loader.google.get_file_as_df(
                row["ID"], "csv",
                read_cols=["SKU", "Price", "Source"],
                dtype={"SKU": str, "Price": float, "Source": str},
            )
            df_dsv_i = df_dsv_i[df_dsv_i["SKU"].isin(top_skus)].copy()
            df_dsv_i = df_dsv_i.drop_duplicates(
                subset=["SKU", "Source"], keep="first"
            )
            df_dsv_i["dsv_date"] = row["dsv_date"]
            dsv_snapshots.append(df_dsv_i)
            if (idx + 1) % 10 == 0 or idx == 0:
                logger.info(
                    "Loaded DSV %d/%d: %s (%d rows)",
                    idx + 1, len(dsv_files), row["Name"], len(df_dsv_i),
                )

        df_dsv_all = pd.concat(dsv_snapshots, ignore_index=True)
        df_dsv_all = df_dsv_all.rename(
            columns={"SKU": "sku", "Price": "cost_to_walmart", "Source": "source"}
        )
        df_dsv_all["source"] = df_dsv_all["source"].astype(str)
        self._df_dsv_all = df_dsv_all

        # merge_asof: node-specific prices
        df_dsv_node = df_dsv_all[df_dsv_all["source"] != "nan"].copy()
        df_dsv_node = df_dsv_node.rename(columns={"source": "node"})
        df_dsv_node = df_dsv_node.sort_values("dsv_date")

        df_dsv_national = df_dsv_all[df_dsv_all["source"] == "nan"].copy()
        df_dsv_national = df_dsv_national[
            ["sku", "cost_to_walmart", "dsv_date"]
        ].sort_values("dsv_date")

        scaffold = scaffold.sort_values("date")
        scaffold = pd.merge_asof(
            scaffold,
            df_dsv_node[["sku", "node", "cost_to_walmart", "dsv_date"]].sort_values(
                "dsv_date"
            ),
            left_on="date",
            right_on="dsv_date",
            by=["sku", "node"],
            direction="backward",
            suffixes=("", "_dsv"),
        )

        # Fill NaN cost_to_walmart with national price fallback
        scaffold_missing = scaffold[scaffold["cost_to_walmart"].isna()].copy()
        if len(scaffold_missing) > 0:
            scaffold_missing = scaffold_missing.drop(
                columns=["cost_to_walmart", "dsv_date"]
            )
            scaffold_missing = pd.merge_asof(
                scaffold_missing.sort_values("date"),
                df_dsv_national.rename(
                    columns={
                        "cost_to_walmart": "cost_to_walmart_nat",
                        "dsv_date": "dsv_date_nat",
                    }
                ),
                left_on="date",
                right_on="dsv_date_nat",
                by="sku",
                direction="backward",
            )
            scaffold.loc[scaffold["cost_to_walmart"].isna(), "cost_to_walmart"] = (
                scaffold_missing["cost_to_walmart_nat"].values
            )

        scaffold = scaffold.drop(columns=["dsv_date"], errors="ignore")
        logger.info(
            "Rows with cost_to_walmart: %.1f%%",
            scaffold["cost_to_walmart"].notna().mean() * 100,
        )
        return scaffold

    # ------------------------------------------------------------------
    # Private: Offer prices (cell 20)
    # ------------------------------------------------------------------

    def _merge_offer_prices(self, scaffold, top_skus):
        df_item_report = self.loader.load(
            "dw_walmart_item_report", date_str=self.end_date
        )
        df_offer = df_item_report[["Product Code", "offer_price"]].copy()
        df_offer = df_offer.rename(columns={"Product Code": "sku"})
        df_offer = df_offer[df_offer["sku"].isin(top_skus)].drop_duplicates(
            subset="sku", keep="first"
        )
        df_offer["offer_price"] = pd.to_numeric(
            df_offer["offer_price"], errors="coerce"
        )

        scaffold = scaffold.merge(df_offer, on="sku", how="left")
        logger.info("Offer prices loaded: %s SKUs", f"{len(df_offer):,}")
        return scaffold

    # ------------------------------------------------------------------
    # Private: Supporting data (cells 22-23)
    # ------------------------------------------------------------------

    def _merge_supporting_data(self, scaffold):
        # MAP prices
        df_map = self.loader.load("dw_map_prices", date_str=self.end_date)
        df_map = df_map.rename(columns={"Product Code": "sku"})
        df_map = df_map[["sku", "MAP"]].drop_duplicates(subset="sku", keep="first")
        df_map["MAP"] = pd.to_numeric(df_map["MAP"], errors="coerce")
        scaffold = scaffold.merge(df_map, on="sku", how="left")

        # Shipping costs
        df_shipping = self.loader.load("shipping_costs_by_node")
        df_shipping = df_shipping.rename(columns={"Identifier": "node"})
        df_shipping["node"] = df_shipping["node"].astype(str)
        scaffold = scaffold.merge(
            df_shipping[["node", "Shipping cost"]], on="node", how="left"
        )
        scaffold = scaffold.rename(columns={"Shipping cost": "shipping_cost"})

        # Warehouse node mapping
        df_wh_mapping = self.loader.load("warehouse_node_mapping")
        df_wh_mapping["Identifier"] = df_wh_mapping["Identifier"].astype(str)
        df_wh_mapping["Warehouse Code"] = df_wh_mapping["Warehouse Code"].astype(str)
        self._df_wh_mapping = df_wh_mapping

        node_to_wh = df_wh_mapping[["Identifier", "Warehouse Code"]].drop_duplicates(
            subset="Identifier"
        )

        # Warehouse addresses -> city/state
        if self.warehouse_addresses_path:
            df_addresses = pd.read_csv(
                self.warehouse_addresses_path, dtype={"Code": str}
            )
            df_city = df_addresses[["Code", "Town", "State"]].rename(
                columns={"Code": "Warehouse Code"}
            )
            node_city = node_to_wh.merge(df_city, on="Warehouse Code", how="left")
            node_city = node_city.rename(columns={"Identifier": "node"})
            scaffold = scaffold.merge(
                node_city[["node", "Town", "State"]], on="node", how="left"
            )
            logger.info(
                "City mapping coverage: %.1f%%",
                scaffold["Town"].notna().mean() * 100,
            )
        else:
            scaffold["Town"] = np.nan
            scaffold["State"] = np.nan
            logger.warning("No warehouse addresses path provided; Town/State = NaN")

        return scaffold

    # ------------------------------------------------------------------
    # Private: Inventory (cells 25-27)
    # ------------------------------------------------------------------

    def _load_and_merge_inventory(self, scaffold, top_skus):
        ensure_modules_path()
        import pricing_module as pricing

        inv_dates = (
            pd.date_range(
                self.start_dt,
                self.end_dt,
                freq=self.cfg["inventory_sample_freq"],
            )
            .strftime("%Y-%m-%d")
            .tolist()
        )
        if self.end_date not in inv_dates:
            inv_dates.append(self.end_date)

        logger.info("Inventory dates to load: %d", len(inv_dates))

        min_units = self.nlc_config["inventory"]["min_units_secondary"]

        df_wh_filt = self._df_wh_mapping[
            (self._df_wh_mapping["Channel"] == "WalmartB2B")
            & (self._df_wh_mapping["Warehouse Status"] == "ENABLED")
            & (self._df_wh_mapping["Identifier Status"] == "ENABLED")
            & (self._df_wh_mapping["Inventory Enabled"] == 1)
        ].copy()

        inv_records = []
        for date_i in inv_dates:
            logger.info("Loading inventory for %s...", date_i)
            df_inv_i = pricing.get_inventory(
                date_i, add_rebates=False, amazon=False, greater3=True
            )
            df_inv_i["date"] = pd.to_datetime(date_i)
            df_inv_i = df_inv_i[df_inv_i["Available"] >= min_units].copy()
            df_inv_i = df_inv_i[df_inv_i["Product Code"].isin(top_skus)].copy()
            df_inv_i["Warehouse Code"] = df_inv_i["Warehouse Code"].astype(str)
            df_inv_i = df_inv_i.merge(
                df_wh_filt[["Warehouse Code", "Identifier", "Inventory Threshold"]],
                on="Warehouse Code",
                how="inner",
            )
            df_inv_i = df_inv_i[
                df_inv_i["Available"] >= df_inv_i["Inventory Threshold"]
            ].copy()

            df_inv_agg = (
                df_inv_i.groupby(["Product Code", "Identifier"])
                .agg({"Purchase Price+FET": "min"})
                .reset_index()
                .rename(
                    columns={
                        "Product Code": "sku",
                        "Identifier": "node",
                        "Purchase Price+FET": "min_purchase_price_fet",
                    }
                )
            )
            df_inv_agg["inv_date"] = pd.to_datetime(date_i)
            df_inv_agg["can_show_inv"] = 1
            inv_records.append(df_inv_agg)
            logger.info("  -> %d SKU-Node pairs with inventory", len(df_inv_agg))

        df_inv_all = pd.concat(inv_records, ignore_index=True)
        df_inv_all["node"] = df_inv_all["node"].astype(str)

        # Forward-fill via merge_asof
        scaffold = scaffold.sort_values("date")
        df_inv_all = df_inv_all.sort_values("inv_date")

        scaffold = pd.merge_asof(
            scaffold,
            df_inv_all[
                ["sku", "node", "min_purchase_price_fet", "can_show_inv", "inv_date"]
            ],
            left_on="date",
            right_on="inv_date",
            by=["sku", "node"],
            direction="backward",
        )
        scaffold["can_show_inv"] = scaffold["can_show_inv"].fillna(0).astype(int)
        scaffold = scaffold.drop(columns=["inv_date"], errors="ignore")

        logger.info(
            "Inventory coverage: %.1f%%",
            (scaffold["can_show_inv"] == 1).mean() * 100,
        )
        return scaffold

    # ------------------------------------------------------------------
    # Private: Computed columns (cell 29)
    # ------------------------------------------------------------------

    def _compute_columns(self, scaffold):
        scaffold["walmart_margin"] = (
            (scaffold["offer_price"] - scaffold["cost_to_walmart"])
            / scaffold["offer_price"]
        )
        scaffold["te_margin"] = (
            (scaffold["cost_to_walmart"] - scaffold["min_purchase_price_fet"])
            / scaffold["cost_to_walmart"]
        )
        scaffold["brand"] = scaffold["sku"].str[:4]
        scaffold["day_of_week"] = scaffold["date"].dt.dayofweek

        scaffold["map_proximity"] = scaffold["cost_to_walmart"] / (
            scaffold["MAP"] * 0.95
        )

        scaffold["min_price_to_show_inv"] = (
            scaffold["min_purchase_price_fet"] / 0.96
        )
        scaffold["can_show_inventory"] = (
            scaffold["cost_to_walmart"] > scaffold["min_price_to_show_inv"]
        ).fillna(False)

        # Number of active nodes per SKU per date
        active_nodes = (
            scaffold[scaffold["can_show_inv"] == 1]
            .groupby(["sku", "date"])["node"]
            .nunique()
            .reset_index()
            .rename(columns={"node": "n_active_nodes"})
        )
        scaffold = scaffold.merge(active_nodes, on=["sku", "date"], how="left")
        scaffold["n_active_nodes"] = scaffold["n_active_nodes"].fillna(0).astype(int)

        logger.info("Computed columns added.")
        return scaffold

    # ------------------------------------------------------------------
    # Private: Days since price change (cell 30)
    # ------------------------------------------------------------------

    def _compute_days_since_price_change(self, scaffold):
        df_dsv_all = self._df_dsv_all
        if df_dsv_all is None or len(df_dsv_all) == 0:
            scaffold["days_since_price_change"] = np.nan
            return scaffold

        dsv_prices = (
            df_dsv_all[df_dsv_all["source"] != "nan"]
            .rename(columns={"source": "node_dsv"})
            .sort_values("dsv_date")
        )
        dsv_prices["prev_price"] = dsv_prices.groupby(["sku", "node_dsv"])[
            "cost_to_walmart"
        ].shift(1)
        dsv_prices["price_changed"] = (
            (dsv_prices["cost_to_walmart"] != dsv_prices["prev_price"])
            & dsv_prices["prev_price"].notna()
        )

        change_dates = dsv_prices[dsv_prices["price_changed"]][
            ["sku", "node_dsv", "dsv_date"]
        ].copy()
        change_dates = change_dates.rename(
            columns={"node_dsv": "node", "dsv_date": "change_date"}
        )
        change_dates = change_dates.sort_values("change_date")

        if len(change_dates) > 0:
            scaffold = scaffold.sort_values("date")
            scaffold = pd.merge_asof(
                scaffold,
                change_dates,
                left_on="date",
                right_on="change_date",
                by=["sku", "node"],
                direction="backward",
            )
            scaffold["days_since_price_change"] = (
                scaffold["date"] - scaffold["change_date"]
            ).dt.days
            scaffold = scaffold.drop(columns=["change_date"], errors="ignore")
        else:
            scaffold["days_since_price_change"] = np.nan

        return scaffold

    # ------------------------------------------------------------------
    # Private: Rolling features (cell 32)
    # ------------------------------------------------------------------

    def _compute_rolling_features(self, scaffold):
        scaffold = scaffold.sort_values(["sku", "node", "date"]).reset_index(
            drop=True
        )
        rolling_cols = self.cfg.get(
            "rolling_cols",
            ["qty_sold", "te_margin", "cost_to_walmart", "offer_price", "walmart_margin"],
        )
        window = self.rolling_window

        # Scaffold is a cross-join (sku_nodes x dates), so all groups have
        # the same number of rows.  Reshape to 2-D numpy for vectorised
        # shift + rolling — avoids 44K-group Python-loop overhead.
        group_sizes = scaffold.groupby(["sku", "node"], sort=False).size()
        n_dates = int(group_sizes.iloc[0])
        uniform = (group_sizes == n_dates).all()

        if uniform and len(scaffold) == len(group_sizes) * n_dates:
            n_groups = len(group_sizes)
            for col in rolling_cols:
                arr = scaffold[col].values.astype(float).reshape(n_groups, n_dates)
                # Shift right by 1 (exclude current day)
                shifted = np.empty_like(arr)
                shifted[:, 0] = np.nan
                shifted[:, 1:] = arr[:, :-1]
                # Rolling mean via cumsum trick (NaN-aware)
                valid = ~np.isnan(shifted)
                vals = np.where(valid, shifted, 0.0)
                cs_vals = np.cumsum(vals, axis=1)
                cs_cnt = np.cumsum(valid.astype(float), axis=1)
                avg = np.full_like(shifted, np.nan)
                for j in range(n_dates):
                    lo = max(0, j - window + 1)
                    s = cs_vals[:, j] - (cs_vals[:, lo - 1] if lo > 0 else 0.0)
                    c = cs_cnt[:, j] - (cs_cnt[:, lo - 1] if lo > 0 else 0.0)
                    mask = c > 0
                    avg[mask, j] = s[mask] / c[mask]
                scaffold[f"{col}_7d_avg"] = avg.ravel()
                scaffold[f"{col}_vs_7d"] = scaffold[col] - scaffold[f"{col}_7d_avg"]
                scaffold[f"{col}_vs_7d_pct"] = scaffold[f"{col}_vs_7d"] / scaffold[
                    f"{col}_7d_avg"
                ].replace(0, np.nan)
        else:
            # Fallback for non-uniform groups
            logger.info("Non-uniform groups detected, using pandas groupby fallback")
            grp = scaffold.groupby(["sku", "node"], sort=False)
            for col in rolling_cols:
                scaffold[f"{col}_7d_avg"] = grp[col].transform(
                    lambda x: x.shift(1).rolling(window, min_periods=1).mean()
                )
                scaffold[f"{col}_vs_7d"] = scaffold[col] - scaffold[f"{col}_7d_avg"]
                scaffold[f"{col}_vs_7d_pct"] = scaffold[f"{col}_vs_7d"] / scaffold[
                    f"{col}_7d_avg"
                ].replace(0, np.nan)

        logger.info("Rolling comparison columns added for: %s", rolling_cols)
        return scaffold

    # ------------------------------------------------------------------
    # Private: Trim warmup (cell 33)
    # ------------------------------------------------------------------

    def _trim_warmup(self, scaffold):
        df = scaffold[scaffold["date"] >= self.start_dt].copy().reset_index(drop=True)
        logger.info(
            "Trimmed warmup: %s -> %s to %s",
            f"{df.shape}", df["date"].min(), df["date"].max(),
        )
        return df

    # ------------------------------------------------------------------
    # Private: Extended features (cells 56-57)
    # ------------------------------------------------------------------

    def _load_tire_size(self, df):
        try:
            df_tireproduct = self.loader.dw.run_query(
                'SELECT code AS "Product Code", full_size '
                "FROM warehouse.d_tireproduct",
                new_credentials=False,
            )
            df_tireproduct = df_tireproduct.drop_duplicates(subset=["Product Code"])
            df_tireproduct["tire_diameter"] = (
                df_tireproduct["full_size"]
                .astype(str)
                .str.extract(r"(\d{2})$")[0]
                .astype(float)
            )
            df_tireproduct.loc[
                ~df_tireproduct["tire_diameter"].between(13, 30), "tire_diameter"
            ] = np.nan
            df_tireproduct["tire_size"] = df_tireproduct["full_size"]

            df = df.merge(
                df_tireproduct[["Product Code", "tire_size", "tire_diameter"]],
                left_on="sku",
                right_on="Product Code",
                how="left",
            )
            # Clean up duplicate columns
            if "Product Code_y" in df.columns:
                df.drop(
                    columns=[c for c in df.columns if c.endswith("_y")], inplace=True
                )
                df.rename(
                    columns={
                        c: c.replace("_x", "")
                        for c in df.columns
                        if c.endswith("_x")
                    },
                    inplace=True,
                )
            elif "Product Code" in df.columns and "Product Code" != "sku":
                df.drop(columns=["Product Code"], inplace=True, errors="ignore")

            logger.info(
                "Tire size coverage: %.1f%%, diameter coverage: %.1f%%",
                df["tire_size"].notna().mean() * 100,
                df["tire_diameter"].notna().mean() * 100,
            )
        except Exception as e:
            logger.warning("Tire size load failed: %s", e)
            df["tire_size"] = np.nan
            df["tire_diameter"] = np.nan

        return df
