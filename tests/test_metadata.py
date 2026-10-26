"""Unit tests for Oracle metadata discovery service and connection error handling."""

from unittest.mock import MagicMock

import oracledb

from oracle_parquet.config import Settings
from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.db.metadata import ColumnMetadata, OracleMetadataService


def test_column_metadata_display_type():
    c1 = ColumnMetadata("C1", "NUMBER", 22, 12, 2, True, 1)
    assert c1.display_type == "NUMBER(12,2)"

    c2 = ColumnMetadata("C2", "VARCHAR2", 50, None, None, False, 2)
    assert c2.display_type == "VARCHAR2(50)"

    c3 = ColumnMetadata("C3", "DATE", 7, None, None, True, 3)
    assert c3.display_type == "DATE"


def test_list_schemas():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.__iter__.return_value = [("HR",), ("SALES",), ("SYSTEM",)]

    schemas = OracleMetadataService.list_schemas(mock_conn)
    assert schemas == ["HR", "SALES", "SYSTEM"]


def test_list_tables():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.__iter__.return_value = [
        ("EMPLOYEES", "TABLE", 107),
        ("EMP_DETAILS_VIEW", "VIEW", None),
    ]

    tables = OracleMetadataService.list_tables(mock_conn, "HR")
    assert len(tables) == 2
    assert tables[0].name == "EMPLOYEES"
    assert tables[0].object_type == "TABLE"
    assert tables[1].name == "EMP_DETAILS_VIEW"
    assert tables[1].object_type == "VIEW"


def test_get_columns_preserves_order():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.__iter__.return_value = [
        ("COL_A", "VARCHAR2", 100, None, None, "N", 1),
        ("COL_B", "NUMBER", 22, 8, 0, "Y", 2),
        ("COL_C", "DATE", 7, None, None, "Y", 3),
    ]

    cols = OracleMetadataService.get_columns(mock_conn, "HR", "DEMO")
    assert [c.name for c in cols] == ["COL_A", "COL_B", "COL_C"]
    assert cols[0].nullable is False
    assert cols[1].nullable is True


def test_connection_test_failure_handling(monkeypatch):
    settings = Settings(
        oracle_user="bad_user",
        oracle_password="bad_password",
        oracle_dsn="unreachable_host:1521/XEPDB1",
        oracle_client_mode="thin",
    )
    mgr = OracleConnectionManager(settings)

    def mock_connect(*args, **kwargs):
        raise oracledb.DatabaseError("ORA-12541: Cannot connect. No listener at host unreachable_host")

    monkeypatch.setattr(oracledb, "connect", mock_connect)
    status = mgr.test_connection()
    assert status.success is False
    assert "bad_password" not in (status.error_message or "")
    assert "ORA-12541" in (status.error_message or "")
