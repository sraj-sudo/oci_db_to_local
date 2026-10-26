"""Oracle database metadata discovery for schemas, tables, and columns."""

from dataclasses import dataclass

import oracledb

from oracle_parquet.logging_config import get_logger

logger = get_logger("db.metadata")


@dataclass(frozen=True)
class ColumnMetadata:
    """Detailed metadata for an individual table/view column."""

    name: str
    data_type: str
    data_length: int | None
    data_precision: int | None
    data_scale: int | None
    nullable: bool
    column_id: int

    @property
    def display_type(self) -> str:
        """Formatted Oracle type with precision/scale or length."""
        dt = self.data_type.upper()
        if self.data_precision is not None:
            if self.data_scale is not None and self.data_scale > 0:
                return f"{dt}({self.data_precision},{self.data_scale})"
            return f"{dt}({self.data_precision})"
        if self.data_length and dt in {"VARCHAR2", "CHAR", "RAW", "NVARCHAR2", "NCHAR"}:
            return f"{dt}({self.data_length})"
        return dt


@dataclass(frozen=True)
class TableMetadata:
    """Metadata for an accessible Oracle table or view."""

    schema: str
    name: str
    object_type: str  # "TABLE" or "VIEW"
    num_rows: int | None = None


class OracleMetadataService:
    """Discovers accessible schemas, tables, views, and column attributes."""

    @staticmethod
    def list_schemas(conn: oracledb.Connection) -> list[str]:
        """Discover database schemas accessible to the current user."""
        sql = """
            SELECT DISTINCT OWNER
            FROM (
                SELECT OWNER FROM ALL_TABLES
                UNION ALL
                SELECT OWNER FROM ALL_VIEWS
            )
            ORDER BY OWNER
        """
        schemas: list[str] = []
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for (owner,) in cursor:
                schemas.append(owner)

        logger.debug("Discovered %d accessible schemas", len(schemas))
        return schemas

    @staticmethod
    def list_tables(conn: oracledb.Connection, schema: str) -> list[TableMetadata]:
        """Retrieve all tables and views belonging to the given schema."""
        sql = """
            SELECT TABLE_NAME, 'TABLE' AS OBJECT_TYPE, NUM_ROWS
            FROM ALL_TABLES
            WHERE OWNER = :schema
            UNION ALL
            SELECT VIEW_NAME AS TABLE_NAME, 'VIEW' AS OBJECT_TYPE, NULL AS NUM_ROWS
            FROM ALL_VIEWS
            WHERE OWNER = :schema
            ORDER BY 1
        """
        results: list[TableMetadata] = []
        with conn.cursor() as cursor:
            cursor.execute(sql, {"schema": schema.upper()})
            for table_name, obj_type, num_rows in cursor:
                results.append(
                    TableMetadata(
                        schema=schema.upper(),
                        name=table_name,
                        object_type=obj_type,
                        num_rows=num_rows,
                    )
                )

        logger.debug("Discovered %d tables/views in schema %s", len(results), schema)
        return results

    @staticmethod
    def get_columns(conn: oracledb.Connection, schema: str, table: str) -> list[ColumnMetadata]:
        """Retrieve column definitions for a given schema and table, preserving column order."""
        sql = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                DATA_LENGTH,
                DATA_PRECISION,
                DATA_SCALE,
                NULLABLE,
                COLUMN_ID
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :table_name
            ORDER BY COLUMN_ID
        """
        columns: list[ColumnMetadata] = []
        with conn.cursor() as cursor:
            cursor.execute(sql, {"schema": schema.upper(), "table_name": table.upper()})
            for col_name, dtype, length, precision, scale, nullable, col_id in cursor:
                columns.append(
                    ColumnMetadata(
                        name=col_name,
                        data_type=dtype,
                        data_length=length,
                        data_precision=precision,
                        data_scale=scale,
                        nullable=(nullable == "Y"),
                        column_id=col_id or (len(columns) + 1),
                    )
                )

        logger.debug("Discovered %d columns for %s.%s", len(columns), schema, table)
        return columns
