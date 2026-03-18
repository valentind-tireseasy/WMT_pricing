"""Thin adapter wrapping the shared DW_connection module.

Provides a clean interface for running SQL queries against the data warehouse.
"""

import logging

from src.adapters.module_loader import ensure_modules_path

logger = logging.getLogger(__name__)

# Module-level reference — populated on first use
_DW = None


def _get_dw():
    """Lazy-import DW_connection."""
    global _DW
    if _DW is None:
        ensure_modules_path()
        import DW_connection as DW
        _DW = DW
    return _DW


class DataWarehouseAdapter:
    """Wrapper around DW_connection with query execution."""

    def __init__(self):
        self._connected = False

    def _ensure_connected(self):
        """Ensure the DW module is loaded (it manages its own connection)."""
        if not self._connected:
            _get_dw()
            self._connected = True
            logger.info("Data warehouse module loaded.")

    def run_query(self, query: str, new_credentials: bool = True):
        """Execute a SELECT query and return results as a DataFrame."""
        self._ensure_connected()
        DW = _get_dw()
        logger.debug("Running query: %s", query[:100] + "..." if len(query) > 100 else query)
        try:
            result = DW.runQuery(query, newCredentials=new_credentials)
        except Exception as e:
            raise RuntimeError(
                f"DW query failed (new_credentials={new_credentials}): {e}\n"
                f"Query: {query[:300]}"
            ) from e
        if result is None:
            alt_creds = not new_credentials
            logger.warning(
                "Query returned None with new_credentials=%s, retrying with %s...",
                new_credentials, alt_creds,
            )
            try:
                result = DW.runQuery(query, newCredentials=alt_creds)
            except Exception as e:
                logger.warning("Fallback also failed: %s", e)
                result = None
            if result is None:
                raise RuntimeError(
                    f"Query returned None with BOTH credential modes. "
                    f"The DW tables may be temporarily unavailable.\n"
                    f"Query: {query[:300]}"
                )
            logger.info("Fallback succeeded with new_credentials=%s", alt_creds)
        return result

    def verify_query(self, query: str):
        """Verify a SQL query runs successfully. Returns (row_count, col_count)."""
        self._ensure_connected()
        DW = _get_dw()
        verify_sql = f"SELECT * FROM ({query.rstrip().rstrip(';')}) AS _verify LIMIT 1"
        logger.debug("Verifying query: %s", verify_sql[:120])
        df = DW.runQuery(verify_sql, newCredentials=True)
        if df is None:
            raise RuntimeError("Query returned None — table may not exist or access denied")
        return len(df), len(df.columns)

    def close(self):
        """Clean up connections."""
        self._connected = False
        logger.info("Data warehouse adapter closed.")
