"""FTP upload response validator for Walmart B2B.

After uploading the DSV via hybris, wait ~3 hours then run this to:
1. Connect to Walmart FTP server
2. Download XML response files for today
3. Parse ingestion status (SUCCESS / errors)
4. Generate summary Excel report
5. Alert if failure rate exceeds threshold

Matches notebook cells 233-241.
"""

import logging
import os
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd

from src.adapters.module_loader import ensure_modules_path, load_yaml

logger = logging.getLogger(__name__)


class FTPValidator:
    """Validate DSV upload responses from Walmart FTP.

    Usage:
        validator = FTPValidator(today_str="2026-03-18")
        validator.download_responses()
        df_results = validator.parse_responses()
        validator.generate_report(df_results)
    """

    def __init__(self, today_str: str = None):
        self._config = load_yaml("nlc_model.yaml")
        self._settings = load_yaml("settings.yaml")
        self.today_str = today_str or pd.to_datetime("today").strftime("%Y-%m-%d")

        ftp_cfg = self._config["ftp"]
        self.ftp_path = ftp_cfg["ftp_path"]
        self.creds_file = ftp_cfg["credentials_file"]
        self.failure_threshold = ftp_cfg["perc_failure_flag"]

        nlc_folder = self._settings["shared_paths"]["nlc_folder"]
        self.output_folder = os.path.join(
            nlc_folder, "Price updates check", self.today_str
        )
        self.xml_folder = os.path.join(self.output_folder, "response_files")

    def download_responses(self):
        """Connect to FTP and download today's response XML files."""
        ensure_modules_path()
        import FTP_Connection as ftp_server

        os.makedirs(self.xml_folder, exist_ok=True)

        creds_path = ftp_server.create_path_cred(file_name=self.creds_file)
        host, port, username, password = ftp_server.read_credentials(creds_path)
        ftp = ftp_server.connect_ftp(host, port, username, password)

        # List files and filter to today's responses
        ftp.cwd(self.ftp_path)
        names = ftp.nlst()
        df_files = pd.DataFrame({"filename": names})

        dt_str = df_files["filename"].str.extract(
            r"_(\d{8}-\d{6})_", expand=False
        )
        df_files["file_datetime"] = pd.to_datetime(
            dt_str, format="%Y%m%d-%H%M%S", errors="coerce"
        )
        df_files["is_response"] = df_files["filename"].str.contains(
            "_response", case=False, na=False
        )
        df_files["file_date"] = df_files["file_datetime"].dt.date

        today_date = pd.to_datetime(self.today_str).date()
        df_today = df_files[
            (df_files["file_date"] >= today_date)
            & (df_files["is_response"])
        ].copy()

        if len(df_today) == 0:
            logger.warning("No response files found for %s", self.today_str)
            return 0

        ftp.cwd(self.ftp_path)
        count = 0
        for fname in df_today["filename"]:
            local_path = os.path.join(self.xml_folder, fname)
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)
            count += 1
            if count % 10 == 0:
                logger.info("Downloaded %d/%d files", count, len(df_today))

        logger.info("Downloaded %d response files to %s", count, self.xml_folder)
        return count

    def parse_responses(self) -> pd.DataFrame:
        """Parse all downloaded XML response files.

        Returns:
            DataFrame with columns: index, productId, shipNode,
            ingestionStatus, error_type, error_code, error_field, error_description
        """
        if not os.path.exists(self.xml_folder):
            logger.warning("XML folder does not exist: %s", self.xml_folder)
            return pd.DataFrame()

        df_all = pd.DataFrame()
        count = 0
        for filename in os.listdir(self.xml_folder):
            filepath = os.path.join(self.xml_folder, filename)
            df_xml = self._read_xml_file(filepath)
            df_all = pd.concat([df_all, df_xml], ignore_index=True)
            count += 1
            if count % 10 == 0:
                logger.info("Parsed %d files", count)

        if len(df_all) == 0:
            logger.warning("No records found in XML files")
            return df_all

        logger.info("Parsed %d total records from %d files", len(df_all), count)

        # Deduplicate: for each SKU-Node, if any SUCCESS exists, it's SUCCESS
        df_all["SKU-Node"] = df_all["productId"] + "-" + df_all["shipNode"]
        df_success = df_all[df_all["ingestionStatus"] == "SUCCESS"].copy()
        df_errors = df_all[df_all["ingestionStatus"] != "SUCCESS"].copy()

        # Errors that never succeeded
        df_errors_only = df_errors[
            ~df_errors["SKU-Node"].isin(df_success["SKU-Node"])
        ].drop_duplicates(subset=["SKU-Node"], keep="last")

        df_unique = pd.concat([df_success, df_errors_only], ignore_index=True)

        # Check failure rate
        n_success = len(df_unique[df_unique["ingestionStatus"] == "SUCCESS"])
        n_fail = len(df_unique[df_unique["ingestionStatus"] != "SUCCESS"])
        total = n_success + n_fail
        fail_rate = n_fail / total if total > 0 else 0

        logger.info(
            "Ingestion: %d success, %d failures (%.2f%% failure rate)",
            n_success, n_fail, fail_rate * 100,
        )

        if fail_rate > self.failure_threshold:
            logger.warning(
                "ALERT: High failure rate %.2f%% exceeds threshold %.2f%%!",
                fail_rate * 100,
                self.failure_threshold * 100,
            )

        return df_unique

    def generate_report(self, df_results: pd.DataFrame) -> str:
        """Generate summary Excel report of ingestion results.

        Returns:
            Path to the output Excel file
        """
        if len(df_results) == 0:
            logger.warning("No results to report")
            return None

        output_path = os.path.join(
            self.output_folder,
            f"NLC Price update response summary {self.today_str}.xlsx",
        )
        os.makedirs(self.output_folder, exist_ok=True)

        df_success = df_results[df_results["ingestionStatus"] == "SUCCESS"]
        df_errors = df_results[df_results["ingestionStatus"] != "SUCCESS"]

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            # Summary sheet
            df_summary = (
                df_results["ingestionStatus"]
                .value_counts()
                .reset_index()
            )
            df_summary.columns = ["ingestionStatus", "Counts"]
            df_summary["Percentage"] = df_summary["Counts"] / df_summary["Counts"].sum()
            df_summary.to_excel(writer, sheet_name="Summary", index=False)

            # Errors by SKU
            if len(df_errors) > 0:
                df_err_by_sku = (
                    df_errors.groupby(["productId", "error_field"])
                    .size()
                    .reset_index(name="count_errors")
                    .sort_values("count_errors", ascending=False)
                )
                df_success_by_sku = (
                    df_success.groupby("productId")
                    .size()
                    .reset_index(name="count_success")
                )
                df_err_by_sku = df_err_by_sku.merge(
                    df_success_by_sku, how="left", on="productId"
                )
                df_err_by_sku["count_success"] = (
                    df_err_by_sku["count_success"].fillna(0).astype(int)
                )
                df_err_by_sku["Has success"] = np.where(
                    df_err_by_sku["count_success"] > 0, "Yes", "No"
                )
                df_err_by_sku.to_excel(
                    writer, sheet_name="Errors by SKU-Reason", index=False
                )

                # Error summary
                df_err_summary = (
                    df_errors["error_description"]
                    .value_counts()
                    .reset_index()
                )
                df_err_summary.columns = ["error_description", "Counts"]
                df_err_summary.to_excel(
                    writer, sheet_name="Error Summary", index=False
                )

                # Full error list
                df_errors.to_excel(writer, sheet_name="Errors list", index=False)

        logger.info("Report saved: %s", output_path)
        return output_path

    @staticmethod
    def _read_xml_file(file_path: str) -> pd.DataFrame:
        """Parse a single Walmart ingestion response XML file."""
        ns_uri = "http://walmart.com/"
        ns = {"ns": ns_uri}
        tag_item_status = f"{{{ns_uri}}}itemIngestionStatus"

        records = []
        for event, elem in ET.iterparse(file_path, events=("end",)):
            if elem.tag == tag_item_status:
                index_elem = elem.find("ns:index", ns)
                product_id_elem = elem.find(".//ns:productId", ns)
                ship_node = elem.find("ns:shipNode", ns)
                status_elem = elem.find("ns:ingestionStatus", ns)

                error_elem = elem.find(".//ns:ingestionError", ns)
                error_type = (
                    error_elem.find("ns:type", ns).text
                    if error_elem is not None
                    and error_elem.find("ns:type", ns) is not None
                    else None
                )
                error_code = (
                    error_elem.find("ns:code", ns).text
                    if error_elem is not None
                    and error_elem.find("ns:code", ns) is not None
                    else None
                )
                error_field = (
                    error_elem.find("ns:field", ns).text
                    if error_elem is not None
                    and error_elem.find("ns:field", ns) is not None
                    else None
                )
                error_description = (
                    error_elem.find("ns:description", ns).text
                    if error_elem is not None
                    and error_elem.find("ns:description", ns) is not None
                    else None
                )

                records.append({
                    "index": index_elem.text if index_elem is not None else None,
                    "productId": (
                        product_id_elem.text
                        if product_id_elem is not None
                        else None
                    ),
                    "shipNode": (
                        ship_node.text if ship_node is not None else None
                    ),
                    "ingestionStatus": (
                        status_elem.text if status_elem is not None else None
                    ),
                    "error_type": error_type,
                    "error_code": error_code,
                    "error_field": error_field,
                    "error_description": error_description,
                })

                elem.clear()

        return pd.DataFrame(records)
