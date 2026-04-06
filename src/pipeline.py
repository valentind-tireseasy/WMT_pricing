"""Main pipeline orchestrator for Walmart NLC pricing.

Runs the full pipeline end-to-end, matching the notebook flow:
1. Load data (DSV, inventory, sales, MAP, warehouse nodes, rollbacks, etc.)
2. Run NLC model (two-pass: min_units=8 then min_units=4)
3. Apply pricing rules (margin split, margin test, low/high updates, new nodes)
4. Build new DSV (apply updates)
5. Update tests tracker
6. Save outputs (DSV CSV + tracker CSV + backup)

Optional toggleable steps:
- run_inventory_check: Compare today vs yesterday inventory costs before pricing
- apply_rollbacks: Remove rollback SKUs from NLC + apply RB prices to national
- update_national_prices: Override national prices from external Excel file

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
from src.notifications.slack_notifier import SlackNotifier

logger = logging.getLogger(__name__)


def _load_dsvd_test_data(dsvd_test_path: str) -> pd.DataFrame:
    """Load DSVD test shipping costs from Excel.

    Returns DataFrame with columns [Identifier, Shipping cost DSVD].
    """
    df = pd.read_excel(dsvd_test_path, dtype={"node": str})
    df = df[df["Target"] == "Yes"].copy()
    df = df.rename(columns={"node": "Identifier"})
    df = df[["Identifier", "Average shipping overall last 30 days"]].copy()
    df = df.rename(columns={"Average shipping overall last 30 days": "Shipping cost DSVD"})
    logger.info("DSVD test data loaded: %d target nodes", len(df))
    return df


def run_pipeline(
    date_str: str = None,
    test: bool = False,
    save: bool = True,
    output_dir: str = None,
    margin_test_start_dates: list = None,
    rollbacks_path: str = None,
    run_inventory_check: bool = False,
    apply_rollbacks: bool = True,
    update_national_prices: bool = False,
    national_prices_path: str = None,
    national_prices_sheet: str = "National prices",
    national_prices_skip_rows: int = 2,
    dsvd_test_path: str = None,
    slack_channel: str = "bot-test",
    slack_enabled: bool = True,
) -> dict:
    """Run the full Walmart NLC pricing pipeline.

    Args:
        date_str: Date string (YYYY-MM-DD). Defaults to today.
        test: If True, include A/B test group updates in DSV.
        save: If True, save DSV and tracker to disk.
        output_dir: Override output directory (for local testing).
        margin_test_start_dates: Filter margin tests by start dates
            (e.g. ["2026-03-12"]).
        run_inventory_check: If True, compare today vs yesterday inventory
            costs before running the NLC model. Diagnostic step.
        rollbacks_path: Full path to the approved rollbacks Excel file.
            Changes monthly. If None, rollbacks are skipped.
        apply_rollbacks: If True, remove rollback SKUs from NLC and apply
            RB prices to national rows. Set False to skip.
        update_national_prices: If True, override national prices from
            an external Excel file. Requires national_prices_path.
        national_prices_path: Path to the Excel file with new national prices.
        national_prices_sheet: Sheet name in the national prices Excel file.
        national_prices_skip_rows: Rows to skip in the national prices sheet.
        dsvd_test_path: Path to the DSVD cost test Excel file. When provided,
            enables DSVD test recurrent updates. When None, DSVD updates are skipped.
        slack_channel: Slack channel to post notifications to.
        slack_enabled: If False, skip all Slack notifications.

    Returns:
        dict with keys: df_output, df_new_dsv, df_tracker, dsv_path,
        tracker_path, df_validation
    """
    if date_str is None:
        date_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    if update_national_prices and not national_prices_path:
        raise ValueError(
            "national_prices_path is required when update_national_prices=True"
        )

    logger.info("=" * 70)
    logger.info("WALMART NLC PRICING PIPELINE")
    logger.info("Date: %s | Test: %s | Inv check: %s | Rollbacks: %s | National prices: %s",
                date_str, test, run_inventory_check, apply_rollbacks, update_national_prices)
    logger.info("=" * 70)

    slack = SlackNotifier(channel=slack_channel, enabled=slack_enabled)
    slack.notify_pipeline_start(date_str, {
        "test": test,
        "inventory_check": run_inventory_check,
        "apply_rollbacks": apply_rollbacks,
        "update_national_prices": update_national_prices,
        "save": save,
    })

    loader = DataLoader()

    inv_check_result = None

    try:
        # ── [Optional] Inventory Check ────────────────────────────────
        if run_inventory_check:
            from src.data.inventory_checker import InventoryChecker

            logger.info("Running inventory check: %s vs previous day...", date_str)
            checker = InventoryChecker(date_current=date_str)
            inv_check_result = checker.run()
            slack.notify_inventory_check(inv_check_result, date_str)
        else:
            logger.info("[Skipped] Inventory check")
            slack.notify_inventory_check_skipped()

        # ── Step 1-2: NLC Model ────────────────────────────────────────
        logger.info("Step 1-2: Running NLC Model...")
        model = NLCModel(date_str=date_str)
        model.load_data(loader, rollbacks_path=rollbacks_path)
        df_output = model.run()
        logger.info("Model complete: %d SKU-Node rows", len(df_output))
        slack.notify_nlc_model(len(df_output))

        # ── Step 3: Pricing Rules ──────────────────────────────────────
        logger.info("Step 3: Applying pricing rules...")
        engine = PricingRulesEngine(
            df_output, model.df_current_tests, date_str, test_mode=test
        )

        df_wm_split_dsv, df_wm_split_tracker = engine.get_wm_margin_split_updates()
        df_margin_dsv, df_margin_tracker = engine.get_margin_test_updates(
            start_dates=margin_test_start_dates
        )
        df_low_dsv, df_low_tracker = engine.get_low_price_updates()
        df_high_dsv, df_high_tracker = engine.get_high_price_updates()
        df_incr_dsv, df_incr_tracker = engine.get_price_increase_test_updates()

        # DSVD test (optional — requires dsvd_test_path)
        if dsvd_test_path:
            df_dsvd_useful = _load_dsvd_test_data(dsvd_test_path)
            df_dsvd_dsv, df_dsvd_tracker = engine.get_dsvd_test_updates(df_dsvd_useful)
        else:
            df_dsvd_dsv, df_dsvd_tracker = pd.DataFrame(), pd.DataFrame()

        df_new_dsv, df_new_tracker = engine.get_new_sku_nodes()

        logger.info("Rules complete.")
        slack.notify_pricing_rules({
            "Wm margin split": len(df_wm_split_dsv),
            "Margin test": len(df_margin_dsv),
            "Price increase test": len(df_incr_dsv),
            "DSVD test": len(df_dsvd_dsv),
            "Low price updates": len(df_low_dsv),
            "High price updates": len(df_high_dsv),
            "New SKU-Nodes": len(df_new_dsv),
        })

        # ── Step 4: Build DSV ──────────────────────────────────────────
        logger.info("Step 4: Building DSV file...")
        builder = DSVBuilder(
            df_curr_dsv_original=model.df_curr_dsv_original,
            today_str=date_str,
        )

        # Prepare base DSV, then apply optional steps
        df_dsv_start = builder._prepare_starting_dsv()

        if update_national_prices:
            logger.info("  [Optional] Updating national prices from: %s",
                        national_prices_path)
            df_dsv_start = builder.apply_national_price_updates(
                df_dsv_start,
                national_prices_path=national_prices_path,
                sheet_name=national_prices_sheet,
                skip_rows=national_prices_skip_rows,
            )
        else:
            logger.info("  [Skipped] Update national prices")
        slack.notify_national_prices(applied=update_national_prices)

        if apply_rollbacks:
            logger.info("  [Optional] Applying rollbacks...")
            df_dsv_start = builder.apply_rollbacks(
                df_dsv_start, model.df_rollbacks
            )
            slack.notify_rollbacks(
                applied=True,
                n_rollbacks=len(model.df_rollbacks),
                n_skus=model.df_rollbacks["Product Code"].nunique(),
            )
        else:
            logger.info("  [Skipped] Rollback handling")
            slack.notify_rollbacks(applied=False)

        list_dsv_updates = [
            df_incr_dsv, df_dsvd_dsv,
            df_wm_split_dsv, df_margin_dsv,
            df_low_dsv, df_high_dsv,
        ]
        df_new_dsv_final = builder.build_from(
            df_dsv_start,
            list_dsv_updates=list_dsv_updates,
            df_new_nodes=df_new_dsv,
        )

        df_validation = builder.validate(df_new_dsv_final)
        validation_counts = (
            df_validation["Price change category"].value_counts().to_dict()
            if df_validation is not None else {}
        )
        slack.notify_dsv_build(len(df_new_dsv_final), validation_counts)

        dsv_path = None
        if save:
            dsv_path = builder.save(df_new_dsv_final, output_path=output_dir)

            # Record last run date for inventory check comparison
            from src.data.inventory_checker import save_last_run_date
            save_last_run_date(date_str)

        # ── Step 5: Update Tracker ─────────────────────────────────────
        logger.info("Step 5: Updating tests tracker...")
        updater = TrackerUpdater(model.df_current_tests, today_str=date_str)
        updater.update_margins(df_output)
        updater.append_entries([
            df_new_tracker,
            df_low_tracker,
            df_high_tracker,
            df_wm_split_tracker,
            df_margin_tracker,
            df_dsvd_tracker,
            df_incr_tracker,
        ])
        slack.notify_tracker_update(len(updater.df_tracker))

        tracker_path = None
        if save:
            tracker_path = updater.save()

        # ── Save notification ──────────────────────────────────────────
        if save:
            slack.notify_save(dsv_path=dsv_path, tracker_path=tracker_path)
        else:
            slack.notify_save(skipped=True)

        # ── Summary ────────────────────────────────────────────────────
        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETE")
        logger.info("  NLC output rows:      %d", len(df_output))
        logger.info("  New DSV rows:         %d", len(df_new_dsv_final))
        logger.info("  Wm margin split:      %d", len(df_wm_split_dsv))
        logger.info("  Margin test:          %d", len(df_margin_dsv))
        logger.info("  Price increase test:  %d", len(df_incr_dsv))
        logger.info("  DSVD test:            %d", len(df_dsvd_dsv))
        logger.info("  Low price updates:    %d", len(df_low_dsv))
        logger.info("  High price updates:   %d", len(df_high_dsv))
        logger.info("  New SKU-Nodes:        %d", len(df_new_dsv))
        logger.info("  Tracker rows:         %d", len(updater.df_tracker))
        if dsv_path:
            logger.info("  DSV saved:            %s", dsv_path)
        if tracker_path:
            logger.info("  Tracker saved:        %s", tracker_path)
        logger.info("=" * 70)

        summary = {
            "nlc_rows": len(df_output),
            "dsv_rows": len(df_new_dsv_final),
            "wm_split": len(df_wm_split_dsv),
            "margin_test": len(df_margin_dsv),
            "price_increase_test": len(df_incr_dsv),
            "dsvd_test": len(df_dsvd_dsv),
            "low_price": len(df_low_dsv),
            "high_price": len(df_high_dsv),
            "new_nodes": len(df_new_dsv),
            "tracker_rows": len(updater.df_tracker),
            "dsv_path": dsv_path,
            "tracker_path": tracker_path,
        }
        slack.notify_pipeline_complete(summary)

        return {
            "inv_check": inv_check_result,
            "df_output": df_output,
            "df_new_dsv": df_new_dsv_final,
            "df_tracker": updater.df_tracker,
            "df_validation": df_validation,
            "dsv_path": dsv_path,
            "tracker_path": tracker_path,
        }

    except Exception as e:
        slack.notify_error("pipeline", e)
        raise

    finally:
        loader.close()


def run_ftp_validation(
    today_str: str = None,
    slack_channel: str = "bot-test",
    slack_enabled: bool = True,
) -> dict:
    """Run FTP response validation (separate from main pipeline).

    Call this ~3 hours after uploading the DSV via hybris.
    """
    from src.dsv.ftp_validator import FTPValidator

    if today_str is None:
        today_str = pd.to_datetime("today").strftime("%Y-%m-%d")

    slack = SlackNotifier(channel=slack_channel, enabled=slack_enabled)

    validator = FTPValidator(today_str=today_str)
    n_files = validator.download_responses()

    if n_files == 0:
        logger.warning("No response files found. Upload may not have completed yet.")
        slack.notify_ftp_validation(0)
        return {"df_results": pd.DataFrame(), "report_path": None}

    df_results = validator.parse_responses()
    report_path = validator.generate_report(df_results)
    slack.notify_ftp_validation(n_files, df_results, report_path)

    return {"df_results": df_results, "report_path": report_path}
