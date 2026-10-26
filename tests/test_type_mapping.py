"""Unit tests for Oracle to PyArrow type mapping and precision preservation."""

import decimal

import pyarrow as pa

from oracle_parquet.db.metadata import ColumnMetadata
from oracle_parquet.extraction.types import OracleTypeMapper


def test_integer_mapping_by_precision():
    c_small = ColumnMetadata("C1", "NUMBER", 22, 4, 0, True, 1)
    f_small, _ = OracleTypeMapper.map_column(c_small)
    assert f_small.type == pa.int16()

    c_med = ColumnMetadata("C2", "NUMBER", 22, 9, 0, True, 2)
    f_med, _ = OracleTypeMapper.map_column(c_med)
    assert f_med.type == pa.int32()

    c_big = ColumnMetadata("C3", "NUMBER", 22, 18, 0, True, 3)
    f_big, _ = OracleTypeMapper.map_column(c_big)
    assert f_big.type == pa.int64()


def test_decimal_mapping():
    c_dec = ColumnMetadata("AMOUNT", "NUMBER", 22, 15, 2, False, 1)
    f_dec, warn = OracleTypeMapper.map_column(c_dec)
    assert f_dec.type == pa.decimal128(15, 2)
    assert f_dec.nullable is False
    assert warn is None


def test_unbounded_number_mapping():
    c_unb = ColumnMetadata("VAL", "NUMBER", 22, None, None, True, 1)
    f_unb, warn = OracleTypeMapper.map_column(c_unb)
    assert f_unb.type == pa.float64()
    assert warn is not None
    assert "Unbounded Oracle NUMBER" in warn.message


def test_string_types():
    for dt in ["VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"]:
        c = ColumnMetadata("STR_COL", dt, 50, None, None, True, 1)
        f, warn = OracleTypeMapper.map_column(c)
        assert f.type == pa.string()
        assert warn is None


def test_lob_and_binary_types():
    c_clob = ColumnMetadata("TEXT", "CLOB", 4000, None, None, True, 1)
    f_clob, warn_clob = OracleTypeMapper.map_column(c_clob)
    assert f_clob.type == pa.large_string()
    assert warn_clob is not None

    c_blob = ColumnMetadata("IMG", "BLOB", 4000, None, None, True, 2)
    f_blob, warn_blob = OracleTypeMapper.map_column(c_blob)
    assert f_blob.type == pa.large_binary()
    assert warn_blob is not None

    c_raw = ColumnMetadata("UUID", "RAW", 16, None, None, True, 3)
    f_raw, _ = OracleTypeMapper.map_column(c_raw)
    assert f_raw.type == pa.binary()


def test_temporal_types():
    c_date = ColumnMetadata("DT", "DATE", 7, None, None, True, 1)
    f_date, _ = OracleTypeMapper.map_column(c_date)
    assert f_date.type == pa.timestamp("us")

    c_ts = ColumnMetadata("TS", "TIMESTAMP", 11, 6, 6, True, 2)
    f_ts, _ = OracleTypeMapper.map_column(c_ts)
    assert f_ts.type == pa.timestamp("us")

    c_tstz = ColumnMetadata("TSTZ", "TIMESTAMP WITH TIME ZONE", 13, 6, 6, True, 3)
    f_tstz, _ = OracleTypeMapper.map_column(c_tstz)
    assert f_tstz.type == pa.timestamp("us", tz="UTC")


def test_sanitize_value_for_arrow():
    dec_type = pa.decimal128(10, 2)
    # Integer to Decimal
    assert OracleTypeMapper.sanitize_value_for_arrow(123, dec_type) == decimal.Decimal("123")
    # Float to Decimal
    assert OracleTypeMapper.sanitize_value_for_arrow(123.45, dec_type) == decimal.Decimal("123.45")
    # None preservation
    assert OracleTypeMapper.sanitize_value_for_arrow(None, dec_type) is None
