"""Node Level Cost pricing model.

Computes optimal node-level costs per SKU-Node based on inventory coverage,
margin targets, and Walmart/MAP price constraints.

This is the core computation engine — equivalent to AmazonPricing's pricing_model.py
but operates at the SKU-Node (warehouse node) level instead of per-SKU.
"""

import logging

import numpy as np
import pandas as pd

from src.adapters.module_loader import load_yaml, ensure_modules_path

logger = logging.getLogger(__name__)


class NLCModel:
    """Compute node-level costs for Walmart B2B pricing.

    Usage:
        model = NLCModel(date_str="2026-03-18")
        model.load_data(loader)
        df_output = model.run()
    """

    def __init__(self, date_str: str, **overrides):
        self._config = load_yaml("nlc_model.yaml")
        self.date_str = date_str

        # Apply overrides
        self.min_wm_margin = overrides.get(
            "min_wm_margin", self._config["margins"]["min_wm_margin"]
        )
        self.distance_map = overrides.get(
            "distance_map", self._config["margins"]["distance_map"]
        )
        self.min_units = overrides.get(
            "min_units", self._config["inventory"]["min_units_default"]
        )
        self.metric_group = self._config["inventory"]["metric_group"]
        self.margin_targets = self._config["margin_targets"]

        # Data attributes — populated by load_data()
        self.df_inventory = None
        self.df_wmt_prices = None
        self.df_map = None
        self.df_wh_node_mapping = None
        self.df_cost_node = None
        self.df_rollbacks = None
        self.df_current_dsv = None
        self.df_current_tests = None
        self.df_sales = None

    def load_data(self, loader):
        """Load all required data sources via the DataLoader.

        Args:
            loader: DataLoader instance
        """
        from dateutil.relativedelta import relativedelta

        date = pd.to_datetime(self.date_str)
        days_sales = self._config["days_sales"]
        start_date = (date - relativedelta(days=days_sales)).strftime("%Y-%m-%d")

        logger.info("Loading NLC model data for date=%s", self.date_str)

        # Google Drive sources
        self.df_current_dsv = loader.load("walmart_dsv_current")
        self.df_wh_node_mapping = loader.load("warehouse_node_mapping")

        # DW sources
        self.df_wmt_prices = loader.load(
            "dw_walmart_item_report", date_str=self.date_str
        )
        self.df_map = loader.load("dw_map_prices", date_str=self.date_str)
        self.df_sales = loader.load(
            "dw_walmart_sales",
            start_date=start_date,
            end_date=self.date_str,
        )

        # Local sources
        self.df_cost_node = loader.load("shipping_costs_by_node")
        self.df_rollbacks = loader.load("rollbacks")
        self.df_current_tests = loader.load("tests_tracker")

        # Inventory via shared pricing module (lazy import)
        ensure_modules_path()
        import pricing_module as pricing
        self.df_inventory = self._load_inventory(pricing, date)

        logger.info("All NLC model data loaded.")

    def _load_inventory(self, pricing, date):
        """Load inventory data for the lookback window using the shared module."""
        days_before = self._config["days_before"]
        dates = pd.date_range(
            end=date, periods=days_before, freq="D"
        ).strftime("%Y-%m-%d").tolist()

        df_all = pd.DataFrame()
        for date_i in dates:
            df_date = pricing.get_inventory(
                date_i, add_rebates=False, amazon=False, greater3=True
            )
            df_date["date"] = pd.to_datetime(date_i)
            logger.info("Inventory loaded for %s: %d rows", date_i, len(df_date))
            df_all = pd.concat([df_all, df_date], ignore_index=True)

        return df_all

    def run(self) -> pd.DataFrame:
        """Run the NLC model and return the output DataFrame.

        Returns:
            DataFrame with columns: Product Code, Identifier, SKU-Node,
            Purchase Price+FET, node-level costs at each margin target,
            Final node level cost, category flags, etc.
        """
        logger.info("Running NLC model (min_units=%d, min_wm_margin=%.3f)",
                     self.min_units, self.min_wm_margin)

        df_output = self._calculate_node_level_costs()

        logger.info("NLC model complete: %d SKU-Node rows", len(df_output))
        return df_output

    def _calculate_node_level_costs(self) -> pd.DataFrame:
        """Core NLC computation.

        Steps:
        1. Process inventory to get per-SKU-Node inventory stats
        2. Merge with Walmart prices and MAP
        3. Compute NLC at each margin target
        4. Determine final NLC based on rules
        5. Categorize changes vs current DSV
        """
        # Step 1: Process inventory
        df_inv = self._process_inventory()

        # Step 2: Merge with price data
        df_avg_wmt = self.df_wmt_prices.groupby("Product Code").agg(
            {"offer_price": "mean", "unit_cost": "mean"}
        ).reset_index()

        df_output = df_inv.merge(df_avg_wmt, how="left", on="Product Code")
        df_output = df_output.merge(self.df_map, how="left", left_on="Product Code",
                                     right_on="sku")
        if "sku" in df_output.columns:
            df_output = df_output.drop(columns=["sku"])

        # Step 3: Compute NLC at each margin target
        for margin in self.margin_targets:
            pct_label = f"{int(margin * 100)}%"
            col_name = f"Node level cost - {pct_label} margin"
            df_output[col_name] = df_output["Purchase Price+FET"] / (1 - margin)

        # Step 4: Determine final NLC
        default_margin = self.margin_targets[0]  # 6% default
        pct_label = f"{int(default_margin * 100)}%"
        df_output["Final node level cost"] = df_output[
            f"Node level cost - {pct_label} margin"
        ]

        # Apply MAP constraint: final price must be <= MAP - distance_map buffer
        if "map" in df_output.columns:
            map_ceiling = df_output["map"] * (1 - self.distance_map)
            has_map = df_output["map"].notna()
            above_map = df_output["Final node level cost"] > map_ceiling
            df_output.loc[has_map & above_map, "Final node level cost"] = map_ceiling

        # Apply Walmart margin constraint
        if self.min_wm_margin > 0:
            has_offer = df_output["offer_price"].notna()
            wm_margin = (df_output["offer_price"] - df_output["Final node level cost"]) / df_output["offer_price"]
            below_wm_min = wm_margin < self.min_wm_margin
            # If our NLC is too high relative to Walmart's margin, don't target this node
            df_output.loc[
                has_offer & below_wm_min,
                "Target for node level cost? - 6% margin"
            ] = "No - Wm margin"

        # Step 5: Categorize changes
        df_output["SKU-Node"] = (
            df_output["Product Code"] + "-" + df_output["Identifier"].astype(str)
        )

        # Merge with current DSV to detect new vs existing
        df_dsv = self.df_current_dsv.copy()
        df_dsv["SKU-Node"] = df_dsv["SKU"] + "-" + df_dsv["Source"].fillna("")
        df_dsv = df_dsv.rename(columns={"Price": "current_nlc_price"})

        df_output = df_output.merge(
            df_dsv[["SKU-Node", "current_nlc_price"]], how="left", on="SKU-Node"
        )

        df_output["New NLC"] = np.where(
            df_output["current_nlc_price"].isna(), "Yes", "No"
        )

        # Compute current NLC margin
        df_output["current_nlc_margin"] = np.where(
            df_output["current_nlc_price"].notna() & (df_output["current_nlc_price"] > 0),
            (df_output["current_nlc_price"] - df_output["Purchase Price+FET"]) / df_output["current_nlc_price"],
            np.nan
        )

        # Price change categorization
        df_output["NLC Price change %"] = np.where(
            df_output["current_nlc_price"].notna() & (df_output["current_nlc_price"] > 0),
            (df_output["Final node level cost"] - df_output["current_nlc_price"]) / df_output["current_nlc_price"],
            np.nan
        )
        df_output["NLC Price change category"] = np.select(
            [
                df_output["NLC Price change %"] > 0.001,
                df_output["NLC Price change %"] < -0.001,
            ],
            ["Increase", "Decrease"],
            default="No change",
        )

        # Margin category
        df_output["current_nlc_margin category"] = pd.cut(
            df_output["current_nlc_margin"],
            bins=[-np.inf, 0.06, 0.11, 0.15, 0.20, np.inf],
            labels=["<6%", "6-11%", "11-15%", "15-20%", ">20%"],
        )

        # Final node level cost category
        df_output["Final node level cost category"] = pd.cut(
            (df_output["Final node level cost"] - df_output["Purchase Price+FET"]) / df_output["Final node level cost"],
            bins=[-np.inf, 0.06, 0.11, 0.15, 0.20, np.inf],
            labels=["<6%", "6%", "11%", "15%", "20%"],
        )

        # Brand code
        df_output["Brand code"] = df_output["Product Code"].str[:4]

        return df_output

    def _process_inventory(self) -> pd.DataFrame:
        """Process raw inventory into per-SKU-Node aggregated stats.

        Filters by min_units, aggregates by Product Code + Identifier (node),
        computes warehouse coverage metrics.
        """
        df = self.df_inventory.copy()

        df["Brand code"] = df["Product Code"].str[:4]

        # Filter by minimum units
        df = df[df["Quantity"] >= self.min_units]

        # Merge with warehouse node mapping
        df = df.merge(
            self.df_wh_node_mapping[["Warehouse Code", "Identifier"]],
            how="left",
            on="Warehouse Code",
        )

        # Drop rows without a node mapping
        df = df.dropna(subset=["Identifier"])

        # Aggregate: per Product Code + Identifier, take min/max/mean of inventory
        agg_dict = {
            "Quantity": self.metric_group,
            "Purchase Price+FET": "first",
            "Warehouse Code": "first",
        }

        if "Node type" in df.columns:
            agg_dict["Node type"] = "first"

        df_agg = df.groupby(["Product Code", "Identifier"]).agg(agg_dict).reset_index()

        df_agg["SKU-Node"] = (
            df_agg["Product Code"] + "-" + df_agg["Identifier"].astype(str)
        )
        df_agg["Min units"] = self.min_units

        # Category based on inventory amount
        df_agg["Category inventory"] = pd.cut(
            df_agg["Quantity"],
            bins=[0, 4, 8, 20, 50, np.inf],
            labels=["1-4", "5-8", "9-20", "21-50", "50+"],
        )

        return df_agg
