"""Main pipeline orchestrator for Walmart NLC pricing.

Runs the full pipeline end-to-end, matching the notebook flow:
1. Load data (DSV, inventory, sales, MAP, warehouse nodes, rollbacks, etc.)
2. Run NLC model (two-pass: min_units=8 then min_units=4)
3. Apply pricing rules (margin split, margin test, low/high updates, new nodes)
4. Build new DSV (apply updates, handle rollbacks)
5. Update tests tracker
6. Save outputs (DSV CSV + tracker CSV + backup)

Optional step 7: FTP validation (run separately, 3 hours after upload)
"""

import logging

import pandas as pd

from src.data.loader import DataLoader
from src.models.nlc_model import NLCModel
from src.rules.pricing_rules import PricingRulesEngine
from src.dsv.dsv_builder import DSVBuilder
from src.tracker.tracker_updater import TrackerUpdater
from src.adapters.module_loader import load_yaml

logger = logging.getLogger(__name__)


def run_pipeline(
    date_str: str = None,
    test: bool = False,
    save: bool = True,
    output_dir: str = None,
    margin_test_start_dates: list = None,
) -> dict:
    """Run the full Walmart NLC pricing pipeline.

    Args:
        date_str: Date string (YYYY-MM-DD). Defaults to today.
        test: If True, include A/B test group updates in DSV.
        save: If True, save DSV and tracker to disk.
        output_dir: Override output directory (for local testing).
        margin_test_start_dates: Filter margin tests by start dates
            (e.g. ["2026-03-12"]).

    Returns:
        dict with keys: df_output, df_new_dsv, df_tracker, dsv_path,
        tracker_path, validation
    """
    if date_str is None:
        date_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    logger.info("=" * 70)
    logger.info("WALMART NLC PRICING PIPELINE")
    logger.info("Date: %s | Test mode: %s", date_str, test)
    logger.info("=" * 70)

    loader = DataLoader()

    try:
        # ── Step 1-2: NLC Model ────────────────────────────────────────
        logger.info("Step 1-2: Running NLC Model (data load + two-pass computation)...")
        model = NLCModel(date_str=date_str)
        model.load_data(loader)
        df_output = model.run()
        logger.info("Model complete: %d SKU-Node rows", len(df_output))

        # ── Step 3: Pricing Rules ──────────────────────────────────────
        logger.info("Step 3: Applying pricing rules...")
        engine = PricingRulesEngine(
            df_output, model.df_current_tests, date_str, test_mode=test
        )

        # 3a: Walmart margin split test
        df_wm_split_dsv, df_wm_split_tracker = engine.get_wm_margin_split_updates()

        # 3b: Brand margin test
        df_margin_dsv, df_margin_tracker = engine.get_margin_test_updates(
            start_dates=margin_test_start_dates
        )

        # 3c: Low price updates (margin < 5.9%)
        df_low_dsv, df_low_tracker = engine.get_low_price_updates()

        # 3d: High price updates (margin > 20.3%)
        df_high_dsv, df_high_tracker = engine.get_high_price_updates()

        # 3e: New SKU-Nodes
        df_new_dsv, df_new_tracker = engine.get_new_sku_nodes()

        logger.info("Rules complete.")

        # ── Step 4: Build DSV ──────────────────────────────────────────
        logger.info("Step 4: Building DSV file...")
        builder = DSVBuilder(
            df_curr_dsv_original=model.df_curr_dsv_original,
            df_rollbacks=model.df_rollbacks,
            today_str=date_str,
        )

        list_dsv_updates = [df_wm_split_dsv, df_margin_dsv, df_low_dsv, df_high_dsv]
        df_new_dsv_final = builder.build(
            list_dsv_updates=list_dsv_updates,
            df_new_nodes=df_new_dsv,
        )

        # Validate
        df_validation = builder.validate(df_new_dsv_final)

        dsv_path = None
        if save:
            dsv_path = builder.save(
                df_new_dsv_final,
                output_path=output_dir,
            )

        # ── Step 5: Update Tracker ─────────────────────────────────────
        logger.info("Step 5: Updating tests tracker...")
        updater = TrackerUpdater(model.df_current_tests, today_str=date_str)

        # Update margins from model output
        updater.update_margins(df_output)

        # Append all tracker entries
        updater.append_entries([
            df_new_tracker,
            df_low_tracker,
            df_high_tracker,
            df_wm_split_tracker,
            df_margin_tracker,
        ])

        tracker_path = None
        if save:
            tracker_path = updater.save()

        # ── Summary ────────────────────────────────────────────────────
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETE")
        logger.info("  NLC output rows:      %d", len(df_output))
        logger.info("  New DSV rows:         %d", len(df_new_dsv_final))
        logger.info("  Wm margin split:      %d", len(df_wm_split_dsv))
        logger.info("  Margin test:          %d", len(df_margin_dsv))
        logger.info("  Low price updates:    %d", len(df_low_dsv))
        logger.info("  High price updates:   %d", len(df_high_dsv))
        logger.info("  New SKU-Nodes:        %d", len(df_new_dsv))
        logger.info(
            "  Tracker rows:         %d", len(updater.df_tracker)
        )
        if dsv_path:
            logger.info("  DSV saved:            %s", dsv_path)
        if tracker_path:
            logger.info("  Tracker saved:        %s", tracker_path)
        logger.info("=" * 70)

        return {
            "df_output": df_output,
            "df_new_dsv": df_new_dsv_final,
            "df_tracker": updater.df_tracker,
            "df_validation": df_validation,
            "dsv_path": dsv_path,
            "tracker_path": tracker_path,
        }

    finally:
        loader.close()


def run_ftp_validation(today_str: str = None) -> dict:
    """Run FTP response validation (separate from main pipeline).

    Call this ~3 hours after uploading the DSV via hybris.

    Args:
        today_str: Date to check responses for.

    Returns:
        dict with keys: df_results, report_path
    """
    from src.dsv.ftp_validator import FTPValidator

    if today_str is None:
        today_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    validator = FTPValidator(today_str=today_str)
    n_files = validator.download_responses()

    if n_files == 0:
        logger.warning("No response files found. Upload may not have completed yet.")
        return {"df_results": pd.DataFrame(), "report_path": None}

    df_results = validator.parse_responses()
    report_path = validator.generate_report(df_results)

    return {"df_results": df_results, "report_path": report_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run_pipeline()
