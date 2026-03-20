"""Slack notifications for the Walmart NLC pricing pipeline.

Posts step-by-step status updates to a Slack channel as the pipeline
runs, including detailed inventory check results and final summary.
"""

import logging
import os

from slack_sdk import WebClient

logger = logging.getLogger(__name__)

SLACK_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
DEFAULT_CHANNEL = "bot-test"


class SlackNotifier:
    """Send pipeline step notifications to Slack.

    Args:
        channel: Slack channel name to post to.
        enabled: If False, all methods become no-ops (for easy toggling).
    """

    def __init__(self, channel: str = DEFAULT_CHANNEL, enabled: bool = True):
        self.channel = channel
        self.enabled = enabled
        self._client = None

    @property
    def client(self) -> WebClient:
        if self._client is None:
            self._client = WebClient(token=SLACK_TOKEN)
        return self._client

    def _post(self, message: str):
        """Post a message to the configured Slack channel."""
        if not self.enabled:
            return
        try:
            self.client.chat_postMessage(
                channel=self.channel,
                text=message,
                username="WMT NLC Pipeline",
            )
        except Exception:
            logger.exception("Failed to post Slack message")

    def notify_pipeline_start(self, date_str: str, flags: dict):
        """Notify that the pipeline has started with its configuration."""
        flag_lines = "\n".join(f"  - {k}: `{v}`" for k, v in flags.items())
        self._post(
            f":rocket: *NLC Pipeline Started*\n"
            f"Date: `{date_str}`\n"
            f"Flags:\n{flag_lines}"
        )

    def notify_inventory_check(self, inv_check_result: dict, date_str: str):
        """Post a detailed inventory check summary to Slack."""
        df_summary = inv_check_result["df_summary"]
        df_vendor = inv_check_result["df_vendor_detail"]

        # Build summary table
        lines = ["*Inventory Cost Comparison*"]
        lines.append(f"Date: `{date_str}` vs previous day\n")
        lines.append("```")
        lines.append(f"{'Category':<25} {'Count SKU-Whs':>15} {'Avg Change %':>15}")
        lines.append("-" * 57)
        for _, row in df_summary.iterrows():
            cat = row["Delta price category"]
            count = f"{int(row['Count SKU-Whs']):,}"
            avg = f"{row['Avg price change %']:.2%}" if row["Avg price change %"] != 0 else "—"
            lines.append(f"{cat:<25} {count:>15} {avg:>15}")
        lines.append("```")

        # Vendor breakdown
        if len(df_vendor) > 0:
            lines.append("\n*Vendor Breakdown (Increases, 1000+ lines)*")
            lines.append("```")
            lines.append(
                f"{'Vendor':<12} {'# Increases':>12} {'Avg Incr %':>12} "
                f"{'Total Lines':>12} {'% Lines':>10}"
            )
            lines.append("-" * 60)
            for _, row in df_vendor.head(15).iterrows():
                vendor = str(row["vendor_code"])[:12]
                count = f"{int(row['Count of wh-sku price Increase']):,}"
                avg = f"{row['Avg price Increase %']:.1%}"
                total = f"{int(row['Total wh-sku lines']):,}"
                pct = f"{row['% Lines Increase']:.1%}"
                lines.append(
                    f"{vendor:<12} {count:>12} {avg:>12} {total:>12} {pct:>10}"
                )
            if len(df_vendor) > 15:
                lines.append(f"  ... and {len(df_vendor) - 15} more vendors")
            lines.append("```")
        else:
            lines.append("\n_No vendor increases above threshold._")

        self._post("\n".join(lines))

    def notify_inventory_check_skipped(self):
        pass

    def notify_nlc_model(self, n_rows: int):
        self._post(
            f":white_check_mark: *Step 1-2: NLC Model Complete*\n"
            f"  Output: `{n_rows:,}` SKU-Node rows"
        )

    def notify_pricing_rules(self, counts: dict):
        """Post pricing rules summary.

        Args:
            counts: dict with keys matching rule names and int values.
        """
        lines = [":white_check_mark: *Step 3: Pricing Rules Complete*"]
        for name, count in counts.items():
            lines.append(f"  - {name}: `{count:,}`")
        self._post("\n".join(lines))

    def notify_dsv_build(self, n_rows: int, validation_counts: dict):
        """Post DSV build summary with validation breakdown."""
        lines = [
            f":white_check_mark: *Step 4: DSV Built*",
            f"  Final DSV: `{n_rows:,}` rows",
        ]
        if validation_counts:
            for cat, count in validation_counts.items():
                lines.append(f"  - {cat}: `{count:,}`")
        self._post("\n".join(lines))

    def notify_national_prices(self, applied: bool):
        if applied:
            self._post(":white_check_mark: *National Prices* — Updated")

    def notify_rollbacks(self, applied: bool, n_rollbacks: int = 0, n_skus: int = 0):
        if applied:
            self._post(
                f":white_check_mark: *Rollbacks* — Applied\n"
                f"  `{n_rollbacks:,}` active rollback rows, `{n_skus:,}` unique SKUs"
            )

    def notify_tracker_update(self, n_rows: int):
        self._post(
            f":white_check_mark: *Step 5: Tracker Updated*\n"
            f"  Total rows: `{n_rows:,}`"
        )

    def notify_save(self, dsv_path: str = None, tracker_path: str = None, skipped: bool = False):
        if skipped:
            return
        lines = [":white_check_mark: *Files Saved*"]
        if dsv_path:
            lines.append(f"  - DSV: `{dsv_path}`")
        if tracker_path:
            lines.append(f"  - Tracker: `{tracker_path}`")
        self._post("\n".join(lines))

    def notify_hybris_upload(self, success: bool = None, skipped: bool = False):
        if skipped:
            return
        if success:
            self._post(":white_check_mark: *Hybris Upload* — Successful")
        else:
            self._post(":x: *Hybris Upload* — Failed or timed out")

    def notify_pipeline_complete(self, summary: dict):
        """Post final pipeline summary."""
        lines = [
            ":tada: *NLC Pipeline Complete*",
            "```",
            f"  NLC output rows:      {summary.get('nlc_rows', 0):>12,}",
            f"  New DSV rows:         {summary.get('dsv_rows', 0):>12,}",
            f"  Wm margin split:      {summary.get('wm_split', 0):>12,}",
            f"  Margin test:          {summary.get('margin_test', 0):>12,}",
            f"  Low price updates:    {summary.get('low_price', 0):>12,}",
            f"  High price updates:   {summary.get('high_price', 0):>12,}",
            f"  New SKU-Nodes:        {summary.get('new_nodes', 0):>12,}",
            f"  Tracker rows:         {summary.get('tracker_rows', 0):>12,}",
            "```",
        ]
        if summary.get("dsv_path"):
            lines.append(f"  DSV: `{summary['dsv_path']}`")
        if summary.get("tracker_path"):
            lines.append(f"  Tracker: `{summary['tracker_path']}`")
        self._post("\n".join(lines))

    def notify_error(self, step: str, error: Exception):
        """Post an error notification."""
        self._post(
            f":x: *Pipeline Error at {step}*\n"
            f"```{type(error).__name__}: {error}```"
        )

    def notify_ftp_validation(self, n_files: int, df_results=None, report_path: str = None):
        """Post FTP validation results."""
        if n_files == 0:
            self._post(
                ":warning: *FTP Validation* — No response files found. "
                "Upload may not have completed yet."
            )
            return

        lines = [f":white_check_mark: *FTP Validation Complete*"]
        if df_results is not None and len(df_results) > 0:
            total = len(df_results)
            status_counts = df_results["ingestionStatus"].value_counts()
            success = status_counts.get("SUCCESS", 0)
            errors = total - success
            rate = (errors / total * 100) if total > 0 else 0
            alert = " :rotating_light:" if rate > 1.5 else ""
            lines.append(f"  Total records: `{total:,}`")
            lines.append(f"  Success: `{success:,}` | Errors: `{errors:,}` | Failure rate: `{rate:.2f}%`{alert}")
        if report_path:
            lines.append(f"  Report: `{report_path}`")
        self._post("\n".join(lines))
