"""Optional integration tests against a live Oracle Database."""

import pytest

from oracle_parquet.config import get_settings
from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.db.metadata import OracleMetadataService


@pytest.mark.integration
def test_live_oracle_connection_and_discovery():
    """Verify live connectivity if configured."""
    settings = get_settings()
    if not settings.is_configured:
        pytest.skip("Live Oracle Database credentials not configured in environment (.env). Skipping.")

    conn_mgr = OracleConnectionManager(settings)
    status = conn_mgr.test_connection()
    if not status.success:
        pytest.skip(f"Live Oracle Database unreachable: {status.error_message}. Skipping integration test.")

    with conn_mgr.get_connection() as conn:
        schemas = OracleMetadataService.list_schemas(conn)
        assert len(schemas) > 0
