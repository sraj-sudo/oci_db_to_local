"""Pytest fixtures and mock objects for Oracle Database connectivity."""

from unittest.mock import MagicMock

import pytest

from oracle_parquet.config import Settings
from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.db.metadata import ColumnMetadata


@pytest.fixture
def mock_settings(tmp_path) -> Settings:
    """Fixture providing clean settings pointing to temp directory."""
    return Settings(
        oracle_user="test_user",
        oracle_password="secret_password",
        oracle_dsn="localhost:1521/TESTDB",
        oracle_client_mode="thin",
        export_dir=tmp_path / "exports",
        batch_size=1000,
        parquet_compression="zstd",
        preview_row_limit=50,
    )


@pytest.fixture
def sample_columns_metadata() -> list[ColumnMetadata]:
    """Sample column metadata set covering various Oracle types."""
    return [
        ColumnMetadata(
            name="ID",
            data_type="NUMBER",
            data_length=22,
            data_precision=10,
            data_scale=0,
            nullable=False,
            column_id=1,
        ),
        ColumnMetadata(
            name="CODE",
            data_type="VARCHAR2",
            data_length=20,
            data_precision=None,
            data_scale=None,
            nullable=False,
            column_id=2,
        ),
        ColumnMetadata(
            name="AMOUNT",
            data_type="NUMBER",
            data_length=22,
            data_precision=14,
            data_scale=2,
            nullable=True,
            column_id=3,
        ),
        ColumnMetadata(
            name="CREATED_AT",
            data_type="TIMESTAMP",
            data_length=11,
            data_precision=6,
            data_scale=6,
            nullable=True,
            column_id=4,
        ),
        ColumnMetadata(
            name="DESCRIPTION",
            data_type="CLOB",
            data_length=4000,
            data_precision=None,
            data_scale=None,
            nullable=True,
            column_id=5,
        ),
    ]


@pytest.fixture
def mock_connection_manager(mock_settings, sample_columns_metadata):
    """Fixture providing an OracleConnectionManager with mocked cursor and connection."""
    mgr = OracleConnectionManager(mock_settings)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock context manager get_connection
    mgr.get_connection = MagicMock()
    mgr.get_connection.return_value.__enter__.return_value = mock_conn

    return mgr
