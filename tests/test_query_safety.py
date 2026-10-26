"""Unit tests for SQL query safety, identifier validation, and injection resistance."""

import pytest

from oracle_parquet.db.queries import SafeQueryBuilder


def test_quote_identifier_valid():
    assert SafeQueryBuilder.quote_identifier("EMPLOYEES") == '"EMPLOYEES"'
    assert SafeQueryBuilder.quote_identifier("HR_SCHEMA_1") == '"HR_SCHEMA_1"'
    assert SafeQueryBuilder.quote_identifier("COL$NAME#") == '"COL$NAME#"'


@pytest.mark.parametrize(
    "malicious_ident",
    [
        "USERS; DROP TABLE USERS;--",
        "SCHEMA' OR '1'='1",
        'TABLE"--',
        "TABLE`",
        "COL NAME WITH SPACE",
        "COL/SLASH",
        "COL\\BACKSLASH",
        "",
        "   ",
        "A" * 150,  # exceeds 128 characters
    ],
)
def test_reject_malicious_identifiers(malicious_ident):
    with pytest.raises(ValueError, match="Invalid Oracle identifier|Identifier cannot be empty"):
        SafeQueryBuilder.validate_identifier(malicious_ident)


def test_build_select_query_basic():
    sql, params = SafeQueryBuilder.build_select_query(
        schema="HR",
        table="EMPLOYEES",
        columns=["EMPLOYEE_ID", "FIRST_NAME", "SALARY"],
    )
    assert sql == 'SELECT "EMPLOYEE_ID", "FIRST_NAME", "SALARY" FROM "HR"."EMPLOYEES"'
    assert params == {}


def test_build_select_query_with_limit():
    sql, params = SafeQueryBuilder.build_select_query(
        schema="HR",
        table="EMPLOYEES",
        columns=["EMPLOYEE_ID"],
        limit=500,
    )
    assert "FETCH FIRST :row_limit ROWS ONLY" in sql
    assert params == {"row_limit": 500}


def test_build_select_query_preserves_order():
    sql, _ = SafeQueryBuilder.build_select_query(
        schema="HR",
        table="EMPLOYEES",
        columns=["SALARY", "EMPLOYEE_ID", "FIRST_NAME"],
    )
    assert sql == 'SELECT "SALARY", "EMPLOYEE_ID", "FIRST_NAME" FROM "HR"."EMPLOYEES"'


def test_build_select_query_column_whitelist_validation():
    valid = ["ID", "NAME", "ACTIVE"]
    with pytest.raises(ValueError, match="Column 'INTRUDER' is not present"):
        SafeQueryBuilder.build_select_query(
            schema="HR",
            table="USERS",
            columns=["ID", "INTRUDER"],
            valid_columns=valid,
        )


def test_build_select_query_empty_columns_rejected():
    with pytest.raises(ValueError, match="At least one column must be selected"):
        SafeQueryBuilder.build_select_query(
            schema="HR",
            table="USERS",
            columns=[],
        )
