"""Config-driven data loader that dispatches to the correct backend.

Reads data_sources.yaml and loads each source via the appropriate adapter
(Google API for sheets/folders, DW for SQL queries, local for Excel/CSV files).
"""

import logging
import re

import pandas as pd

from src.adapters.module_loader import load_yaml
from src.adapters.google_api_adapter import GoogleAPIAdapter
from src.adapters.dw_adapter import DataWarehouseAdapter

logger = logging.getLogger(__name__)


class DataLoader:
    """Load data sources defined in config/data_sources.yaml.

    Usage:
        loader = DataLoader()
        df = loader.load("warehouse_node_mapping")
        df = loader.load("dw_walmart_item_report", date_str="2026-03-18")
    """

    def __init__(self):
        self._config = load_yaml("data_sources.yaml")
        self._sources = self._config["sources"]
        self._google = None
        self._dw = None

    @property
    def google(self) -> GoogleAPIAdapter:
        """Lazy-initialize Google API adapter."""
        if self._google is None:
            self._google = GoogleAPIAdapter()
        return self._google

    @property
    def dw(self) -> DataWarehouseAdapter:
        """Lazy-initialize Data Warehouse adapter."""
        if self._dw is None:
            self._dw = DataWarehouseAdapter()
        return self._dw

    @property
    def source_names(self) -> list:
        """List all configured source names."""
        return list(self._sources.keys())

    def get_source_config(self, name: str) -> dict:
        """Get the configuration dict for a named source."""
        if name not in self._sources:
            raise KeyError(f"Unknown data source: {name}")
        return self._sources[name]

    def load(self, name: str, **overrides):
        """Load a data source by name.

        Args:
            name: Source name from data_sources.yaml
            **overrides: Override template params (e.g. date_str="2026-03-18")

        Returns:
            pandas DataFrame
        """
        config = self.get_source_config(name)
        source_type = config["type"]

        logger.info("Loading source: %s (type=%s)", name, source_type)

        if source_type == "sheet":
            df = self._load_sheet(config)
        elif source_type == "sheets_api":
            df = self._load_sheets_api(config)
        elif source_type == "folder":
            df = self._load_folder(config)
        elif source_type == "sql":
            df = self._load_sql(config, overrides)
        elif source_type == "local":
            df = self._load_local(config, overrides)
        else:
            raise ValueError(f"Unknown source type '{source_type}' for {name}")

        # Apply column renames if configured
        if "column_renames" in config and df is not None:
            df = df.rename(columns=config["column_renames"])

        return df

    def _load_sheet(self, config: dict):
        """Load a Google Sheet source."""
        kwargs = {}
        if "dtype" in config:
            kwargs["dtype"] = config["dtype"]
        if "sheet_name" in config:
            kwargs["sheet_name"] = config["sheet_name"]
        return self.google.get_sheet(config["id"], **kwargs)

    def _load_sheets_api(self, config: dict):
        """Load a Google Sheet via the Sheets API."""
        sheet_gid = config.get("sheet_gid", "0")
        return self.google.get_sheet_via_sheets_api(config["id"], sheet_gid)

    def _load_folder(self, config: dict):
        """Load the latest file from a Google Drive folder."""
        kwargs = {}
        if config.get("csv_separator"):
            kwargs["sep"] = config["csv_separator"]
        if config.get("dtype"):
            kwargs["dtype"] = config["dtype"]
        if config.get("read_cols"):
            kwargs["read_cols"] = config["read_cols"]
        return self.google.get_folder_latest_file(config["id"], **kwargs)

    def _load_sql(self, config: dict, overrides: dict):
        """Load via a SQL query against the data warehouse."""
        query = config["query"]

        template_params = config.get("template_params", [])
        for param in template_params:
            if param in overrides:
                query = query.replace(f"{{{param}}}", overrides[param])
            else:
                raise ValueError(
                    f"Template parameter '{param}' required but not provided. "
                    f"Pass it as: loader.load('{config.get('dataframe', '?')}', {param}='value')"
                )

        new_creds = config.get("new_credentials", True)
        return self.dw.run_query(query, new_credentials=new_creds)

    def _load_local(self, config: dict, overrides: dict):
        """Load a local file (Excel or CSV) from the shared drive."""
        path = config.get("path", "")

        # Substitute any template params in the path
        for key, value in overrides.items():
            path = path.replace(f"{{{key}}}", value)

        kwargs = {}
        if "dtype" in config:
            kwargs["dtype"] = config["dtype"]
        if "usecols" in config:
            kwargs["usecols"] = config["usecols"]

        if path.endswith(".csv"):
            return pd.read_csv(path, **kwargs)
        elif path.endswith(".xlsx") or path.endswith(".xls"):
            return pd.read_excel(path, **kwargs)
        else:
            raise ValueError(f"Unsupported local file format: {path}")

    def load_dsv_by_date(self, date_str: str = None) -> pd.DataFrame:
        """Load a specific DSV file from the folder by date.

        Replicates the notebook's read_dsv() function:
        - Lists all files in the DSV folder
        - Parses dates from filenames (YYYY-MM-DD pattern)
        - Filters to files after min_date_filter
        - Picks the file matching date_str (or max date if None)
        - Reads with columns: SKU, Price, Source
        - Deduplicates by SKU+Source
        """
        config = self.get_source_config("walmart_dsv_folder")
        nlc_config = load_yaml("nlc_model.yaml")
        min_date = nlc_config["dsv"].get("min_date_filter", "2025-12-30")

        folder_id = config["id"]
        files = self.google.get_folder_files(folder_id)

        # Parse dates from filenames
        files["Date"] = files["Name"].apply(
            lambda x: re.search(r"\d{4}-\d{2}-\d{2}", str(x))
        )
        files = files[files["Date"].notna()].copy()
        files["Date"] = files["Date"].apply(lambda m: m.group())
        files["Date"] = pd.to_datetime(files["Date"], format="%Y-%m-%d")
        files = files[files["Date"] >= min_date]

        if date_str is None:
            date_str_dt = files["Date"].max()
            logger.info("Max DSV date: %s", date_str_dt)
        else:
            date_str_dt = pd.to_datetime(date_str)

        files = files[files["Date"] == date_str_dt]
        if len(files) == 0:
            raise FileNotFoundError(f"No DSV file found for date {date_str_dt}")

        file_id = files.iloc[0]["ID"]
        file_name = files.iloc[0]["Name"]
        logger.info("Reading DSV file: %s", file_name)

        dtype = {"SKU": str, "Price": float, "Source": str}
        read_cols = ["SKU", "Price", "Source"]

        df = self.google.get_file_as_df(
            file_id, "csv", read_cols=read_cols, dtype=dtype
        )

        # Deduplicate
        df = df.drop_duplicates(subset=["SKU", "Source"], keep="first")

        # Rename to internal format
        df = df.rename(columns={
            "SKU": "sku",
            "Price": "walmart_dsv_price",
            "Source": "source",
        })

        return df

    def close(self):
        """Close all adapter connections."""
        if self._google:
            self._google.close()
        if self._dw:
            self._dw.close()
