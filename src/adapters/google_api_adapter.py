"""Thin adapter wrapping the shared GoogleAPI_functions module.

Manages the Google API service connection lifecycle and provides
a clean interface for reading sheets, folders, and custom data.
"""

import logging
import time

from src.adapters.module_loader import ensure_modules_path, load_yaml

logger = logging.getLogger(__name__)

# Google API scopes (same as original scripts)
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Module-level reference — populated on first use
_gAPI = None


def _get_gapi():
    """Lazy-import GoogleAPI_functions to avoid triggering OAuth at module load."""
    global _gAPI
    if _gAPI is None:
        ensure_modules_path()
        import GoogleAPI_functions as gAPI
        _gAPI = gAPI
    return _gAPI


class GoogleAPIAdapter:
    """Wrapper around GoogleAPI_functions with connection management."""

    def __init__(self):
        self._service = None
        self._service_sheets = None
        self._settings = load_yaml("settings.yaml")
        self._rate_limit_sleep = self._settings.get("google_api", {}).get(
            "rate_limit_sleep", 0.5
        )

    @property
    def service(self):
        """Lazy-initialize the Google Drive API service connection."""
        if self._service is None:
            self._connect()
        return self._service

    @property
    def service_sheets(self):
        """Lazy-initialize the Google Sheets API service connection."""
        if self._service_sheets is None:
            self._connect()
        return self._service_sheets

    def _connect(self):
        """Establish Google API connections using the shared module."""
        gAPI = _get_gapi()
        logger.info("Initializing Google API service connection...")
        credentials_file = gAPI.create_path_cred()
        self._service, self._service_sheets = gAPI.connect_drive(
            SCOPES, credentials_file
        )
        logger.info("Google API service connected.")

    def _sleep(self):
        """Rate limit protection between API calls."""
        time.sleep(self._rate_limit_sleep)

    def get_sheet(self, file_id: str, *, dtype=None, read_cols=None,
                  sheet_name=None):
        """Read a Google Sheet as a DataFrame."""
        gAPI = _get_gapi()
        kwargs = {}
        if dtype is not None:
            kwargs["dtype"] = dtype
        if read_cols is not None:
            kwargs["read_cols"] = read_cols
        if sheet_name is not None:
            kwargs["sheet_name"] = sheet_name

        logger.debug("Reading sheet %s (sheet_name=%s)", file_id, sheet_name)
        df = gAPI.get_df_of_file(self.service, file_id, "spreadsheet", **kwargs)
        self._sleep()
        return df

    def get_folder_files(self, folder_id: str):
        """List all files in a Google Drive folder."""
        gAPI = _get_gapi()
        logger.debug("Listing files in folder %s", folder_id)
        files = gAPI.get_all_files_folder(self.service, folder_id)
        self._sleep()
        return files

    def get_file_as_df(self, file_id: str, file_type: str, **kwargs):
        """Read a single file from Google Drive as a DataFrame."""
        gAPI = _get_gapi()
        logger.debug("Reading file %s (type=%s)", file_id, file_type)
        df = gAPI.get_df_of_file(self.service, file_id, file_type, **kwargs)
        self._sleep()
        return df

    def get_folder_latest_file(self, folder_id: str, file_type: str = "csv",
                               **kwargs):
        """Get the most recent file from a Drive folder as a DataFrame."""
        gAPI = _get_gapi()
        logger.debug("Getting latest file from folder %s", folder_id)
        df = gAPI.get_last_df(self.service, folder_id, **kwargs)
        self._sleep()
        return df

    def get_sheet_via_sheets_api(self, spreadsheet_id: str, sheet_gid: str = "0",
                                sheet_name: str = None):
        """Read a Google Sheet via the Sheets API."""
        gAPI = _get_gapi()
        logger.debug("Reading sheet %s via Sheets API (gid=%s)", spreadsheet_id,
                      sheet_gid)
        df = gAPI.get_df_gsheet_specific_sheet(
            self.service_sheets, spreadsheet_id, sheet_gid,
            sheet_name=sheet_name
        )
        self._sleep()
        return df

    def close(self):
        """Clean up the service connection."""
        self._service = None
        self._service_sheets = None
        logger.info("Google API service connection closed.")
