"""Hybris DSV upload automation for Walmart B2B.

Automates the manual step of uploading the DSV CSV file to hybris backoffice.
Uses Selenium via the shared hybris module to:
1. Sign into hybris
2. Navigate to the DSV Prices page (/tiresbackoffice/dsv-prices)
3. Select WalmartB2B - EXTERNAL_WAREHOUSE channel
4. Choose the CSV file via the "Choose File" input
5. Click "Upload" button
6. Wait for the upload to process and verify result in the history table

UI layout (from the DSV Prices page):
- Channel dropdown (left panel)
- Upload file section (right panel): "Choose File" input + green "Upload" button
- "Full sync" toggle
- History table: Code | Start Time | End Time | Status | Result | File (Download)

NOTE: hybris requires Selenium/Chrome — only import this module when needed
(never at startup).
"""

import logging
import os
import shutil
import time

from src.adapters.module_loader import ensure_modules_path, load_yaml

logger = logging.getLogger(__name__)

HYBRIS_DSV_URL = "https://cockpits.tires-easy.com/tiresbackoffice/dsv-prices"
WALMART_CHANNEL = "WalmartB2B - EXTERNAL_WAREHOUSE"

# Time to wait for the upload to process (hybris processes take ~40 minutes
# based on the history table timestamps, but the page updates faster)
UPLOAD_POLL_INTERVAL = 300  # seconds between status checks (5 minutes)
UPLOAD_TIMEOUT = 3600       # 1 hour max wait


class HybrisUploader:
    """Upload a DSV CSV file to hybris backoffice.

    Usage:
        uploader = HybrisUploader()
        success = uploader.upload("/path/to/DSV.csv")
        uploader.close()

    Or as a context manager:
        with HybrisUploader() as uploader:
            success = uploader.upload(dsv_path)
    """

    def __init__(self, headless: bool = False):
        """Initialize the uploader.

        Args:
            headless: If True, run Chrome in headless mode (no visible browser).
                      Default False so you can watch/intervene if needed.
        """
        self._config = load_yaml("nlc_model.yaml")
        self._driver = None
        self._headless = headless

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _sign_in(self):
        """Sign into hybris using the shared module."""
        ensure_modules_path()
        import hybris

        logger.info("Signing into hybris...")
        self._driver = hybris.sign_in_hybris(runInServer=self._headless)
        self._driver.maximize_window()
        logger.info("Signed into hybris.")

    def _navigate_to_dsv_page(self):
        """Navigate to the DSV Prices page and select WalmartB2B channel."""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import Select, WebDriverWait

        driver = self._driver
        wait = WebDriverWait(driver, 15)

        # Navigate to DSV prices page via sidebar link
        logger.info("Navigating to DSV Prices page...")
        try:
            dsv_link = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                "//a[@class='menu-button' and @href='/tiresbackoffice/dsv-prices']"
            )))
            dsv_link.click()
        except Exception:
            logger.info("Sidebar link not found, navigating directly...")
            driver.get(HYBRIS_DSV_URL)

        time.sleep(3)

        # Select WalmartB2B channel from dropdown
        logger.info("Selecting channel: %s", WALMART_CHANNEL)
        dropdown = wait.until(EC.element_to_be_clickable(
            (By.ID, "channel-dropdown")
        ))
        select = Select(dropdown)
        select.select_by_visible_text(WALMART_CHANNEL)

        time.sleep(5)
        logger.info("Channel selected.")

    def _get_latest_upload_code(self) -> str:
        """Read the Code from the first row of the upload history table.

        Returns the Code string (e.g. "DSVPriceUpload-WalmartB2B-2026-03-18_06-01-11")
        or None if the table is empty.
        """
        from selenium.webdriver.common.by import By

        driver = self._driver
        try:
            first_row = driver.find_element(
                By.CSS_SELECTOR, ".j-dsv-prices-table-body .j-table-row"
            )
            code_cell = first_row.find_element(By.CSS_SELECTOR, "td:first-child")
            return code_cell.text.strip()
        except Exception:
            return None

    def _get_latest_upload_status(self) -> tuple:
        """Read Status and Result from the first row of the history table.

        Returns (status, result) e.g. ("FINISHED", "SUCCESS") or (None, None).
        """
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup

        driver = self._driver
        try:
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            table = soup.find("table", class_="j-dsv-prices-table")
            if table is None:
                return None, None
            first_row = table.find("tr", class_="j-table-row")
            if first_row is None:
                return None, None
            cells = first_row.find_all("td")
            # Columns: Code | Start Time | End Time | Status | Result | File
            if len(cells) >= 5:
                status = cells[3].text.strip()
                result = cells[4].text.strip()
                return status, result
        except Exception as e:
            logger.warning("Could not read upload status: %s", e)
        return None, None

    def upload(self, dsv_path: str, wait_for_result: bool = True) -> bool:
        """Upload a DSV CSV file to hybris.

        Args:
            dsv_path: Absolute path to the DSV CSV file to upload.
            wait_for_result: If True, poll the history table until the upload
                finishes (FINISHED status) or times out. If False, return
                immediately after clicking Upload.

        Returns:
            True if upload succeeded (Result=SUCCESS), False otherwise.
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        if not os.path.isfile(dsv_path):
            raise FileNotFoundError(f"DSV file not found: {dsv_path}")

        dsv_path = os.path.abspath(dsv_path)
        logger.info("Starting hybris DSV upload: %s", dsv_path)

        # Sign in and navigate
        self._sign_in()
        self._navigate_to_dsv_page()

        driver = self._driver
        wait = WebDriverWait(driver, 30)

        # Record the current latest upload code so we can detect the new one
        prev_code = self._get_latest_upload_code()
        logger.info("Previous latest upload: %s", prev_code)

        # Find the file input ("Choose File" button)
        # The page has an <input type="file"> inside the "Upload file" section
        logger.info("Selecting file...")
        file_input = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'input[type="file"]')
        ))
        file_input.send_keys(dsv_path)
        time.sleep(2)
        logger.info("File selected: %s", os.path.basename(dsv_path))

        # Click the green "Upload" button
        logger.info("Clicking Upload button...")
        upload_btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '//button[normalize-space(text())="Upload"]')
        ))
        upload_btn.click()
        logger.info("Upload button clicked.")

        if not wait_for_result:
            logger.info(
                "wait_for_result=False — returning immediately. "
                "Check hybris manually for upload status."
            )
            return True

        # Poll the history table until a new entry appears and finishes
        logger.info("Waiting for upload to process (timeout=%ds)...", UPLOAD_TIMEOUT)
        start_time = time.time()

        while time.time() - start_time < UPLOAD_TIMEOUT:
            time.sleep(UPLOAD_POLL_INTERVAL)

            # Refresh the page to get updated table
            driver.refresh()
            time.sleep(5)

            # Re-select channel after refresh
            try:
                from selenium.webdriver.support.ui import Select
                dropdown = wait.until(EC.element_to_be_clickable(
                    (By.ID, "channel-dropdown")
                ))
                select = Select(dropdown)
                select.select_by_visible_text(WALMART_CHANNEL)
                time.sleep(5)
            except Exception:
                pass

            current_code = self._get_latest_upload_code()
            status, result = self._get_latest_upload_status()

            elapsed = int(time.time() - start_time)
            logger.info(
                "  [%ds] Latest: %s | Status: %s | Result: %s",
                elapsed, current_code, status, result,
            )

            # Check if a new upload appeared and finished
            if current_code and current_code != prev_code:
                if status == "FINISHED":
                    if result == "SUCCESS":
                        logger.info("Upload completed successfully: %s", current_code)
                        return True
                    else:
                        logger.error(
                            "Upload finished with result: %s (code: %s)",
                            result, current_code,
                        )
                        self._save_debug_screenshot("upload_failed")
                        return False
                # Still processing — continue polling

        logger.error("Upload timed out after %ds.", UPLOAD_TIMEOUT)
        self._save_debug_screenshot("upload_timeout")
        return False

    def _save_debug_screenshot(self, name: str):
        """Save a screenshot for debugging."""
        try:
            settings = load_yaml("settings.yaml")
            nlc_folder = settings["shared_paths"]["nlc_folder"]
            path = os.path.join(nlc_folder, f"hybris_debug_{name}.png")
            self._driver.save_screenshot(path)
            logger.info("Debug screenshot saved: %s", path)
        except Exception as e:
            logger.warning("Could not save debug screenshot: %s", e)

    def close(self):
        """Close the browser."""
        if self._driver is not None:
            self._driver.quit()
            self._driver = None
            logger.info("Hybris browser closed.")


def copy_dsv_to_archive(dsv_path: str) -> str:
    """Copy the uploaded DSV file to the shared drive archive folder.

    Reads the target folder from config/settings.yaml → shared_paths.dsv_archive_folder.

    Args:
        dsv_path: Absolute path to the DSV file that was uploaded.

    Returns:
        The destination path where the file was copied.

    Raises:
        FileNotFoundError: If the DSV file or archive folder doesn't exist.
    """
    if not os.path.isfile(dsv_path):
        raise FileNotFoundError(f"DSV file not found: {dsv_path}")

    settings = load_yaml("settings.yaml")
    archive_folder = settings["shared_paths"]["dsv_archive_folder"]

    if not os.path.isdir(archive_folder):
        os.makedirs(archive_folder, exist_ok=True)
        logger.info("Created archive folder: %s", archive_folder)

    dest_path = os.path.join(archive_folder, os.path.basename(dsv_path))
    shutil.copy2(dsv_path, dest_path)
    logger.info("DSV copied to archive: %s", dest_path)
    return dest_path
