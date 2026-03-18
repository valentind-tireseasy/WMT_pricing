"""Main pipeline orchestrator for Walmart NLC pricing.

Runs the full pipeline end-to-end:
1. NLC Model — compute node-level costs
2. Rules Engine — categorize updates
3. Price Updates — merge all update types
4. DSV Builder — generate new DSV file
5. Tracker Updater — update tests tracker
"""

import logging
from datetime import datetime

import pandas as pd

from src.data.loader import DataLoader
from src.models.nlc_model import NLCModel
from src.rules.pricing_rules import PricingRulesEngine
from src.nlc.price_updates import PriceUpdateManager
from src.dsv.dsv_builder import DSVBuilder
from src.tracker.tracker_updater import TrackerUpdater
from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


def run_pipeline(
    date_str: str = None,
    min_units: int = 4,
    test: bool = False,
    save_dsv: bool = True,
    save_tracker: bool = True,
    output_dir: str = None,
    margin_test_start_dates: list = None,
    **overrides,
) -> dict:
    """Run the full Walmart NLC pricing pipeline.

    Args:
        date_str: Date string (YYYY-MM-DD). Defaults to today.
        min_units: Minimum inventory units for NLC eligibility.
        test: If True, include test group updates in DSV.
        save_dsv: If True, save the DSV CSV to disk.
        save_tracker: If True, save the updated tracker to disk.
        output_dir: Override output directory (for local testing).
        margin_test_start_dates: Filter margin tests by start dates.
        **overrides: Additional parameter overrides for the NLC model.

    Returns:
        dict with keys: df_output, df_new_dsv, df_tracker, dsv_path, tracker_path
    """
    if date_str is None:
        date_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    logger.info("=" * 70)
    logger.info("WALMART NLC PRICING PIPELINE")
    logger.info("Date: %s | Min units: %d | Test mode: %s", date_str, min_units, test)
    logger.info("=" * 70)

    loader = DataLoader()

    try:
        # ── Step 1: NLC Model ──────────────────────────────────────────
        logger.info("Step 1: Running NLC Model...")
        model = NLCModel(date_str=date_str, min_units=min_units, **overrides)
        model.load_data(loader)
        df_output = model.run()
        logger.info("Step 1 complete: %d SKU-Node rows", len(df_output))

        # ── Step 2: Rules Engine ───────────────────────────────────────
        logger.info("Step 2: Applying pricing rules...")
        df_current_tests = loader.load("tests_tracker")
        engine = PricingRulesEngine(df_output, df_current_tests)

        df_new_nodes = engine.get_new_sku_nodes()
        df_low_updates = engine.get_low_price_updates()
        df_high_updates = engine.get_high_price_updates()
        df_wm_split = engine.get_wm_margin_split_updates(today_str=date_str)
        df_margin_test = engine.get_margin_test_updates(
            start_dates=margin_test_start_dates
        )
        logger.info("Step 2 complete.")

        # ── Step 3: Merge price updates ────────────────────────────────
        logger.info("Step 3: Merging price updates...")
        manager = PriceUpdateManager(df_output, df_current_tests, date_str)
        manager.add_updates("wm_margin_split", df_wm_split)
        manager.add_updates("margin_test", df_margin_test)
        manager.add_updates("low_price", df_low_updates)
        manager.add_updates("high_price", df_high_updates)
        manager.add_new_sku_nodes(df_new_nodes)

        df_all_updates = manager.get_all_updates_dsv()
        df_new_nodes_dsv = manager.get_new_nodes_dsv()
        logger.info("Step 3 complete: %d updates, %d new nodes",
                     len(df_all_updates), len(df_new_nodes_dsv))

        # ── Step 4: Build DSV ──────────────────────────────────────────
        logger.info("Step 4: Building DSV file...")
        df_current_dsv = model.df_current_dsv
        df_rollbacks = model.df_rollbacks

        builder = DSVBuilder(
            df_current_dsv=df_current_dsv,
            df_updates=df_all_updates,
            df_new_nodes=df_new_nodes_dsv,
            df_rollbacks=df_rollbacks,
            today_str=date_str,
        )
        df_new_dsv = builder.build()

        # Validate
        checks = builder.validate(df_new_dsv)
        if not all(c["pass"] for c in checks.values()):
            logger.warning("DSV validation has failures — review before uploading!")

        dsv_path = None
        if save_dsv:
            save_to = output_dir if output_dir else None
            dsv_path = builder.save(df_new_dsv, output_path=save_to)

        logger.info("Step 4 complete.")

        # ── Step 5: Update tracker ─────────────────────────────────────
        logger.info("Step 5: Updating tests tracker...")
        df_tracker_updates = manager.get_tracker_updates()
        updater = TrackerUpdater(df_current_tests, today_str=date_str)
        updater.update_margins(df_output)
        updater.append_new_entries(df_tracker_updates)

        tracker_path = None
        if save_tracker:
            tracker_path = updater.save()

        logger.info("Step 5 complete.")

        # ── Summary ────────────────────────────────────────────────────
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETE")
        logger.info("  NLC output rows:  %d", len(df_output))
        logger.info("  New DSV rows:     %d", len(df_new_dsv))
        logger.info("  Price updates:    %d", len(df_all_updates))
        logger.info("  New SKU-Nodes:    %d", len(df_new_nodes_dsv))
        logger.info("  Tracker rows:     %d", len(updater.df_tracker))
        if dsv_path:
            logger.info("  DSV saved:        %s", dsv_path)
        if tracker_path:
            logger.info("  Tracker saved:    %s", tracker_path)
        logger.info("=" * 70)

        return {
            "df_output": df_output,
            "df_new_dsv": df_new_dsv,
            "df_tracker": updater.df_tracker,
            "dsv_path": dsv_path,
            "tracker_path": tracker_path,
            "validation": checks,
        }

    finally:
        loader.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run_pipeline()
