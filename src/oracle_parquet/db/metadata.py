"""Oracle database metadata discovery for schemas, tables, columns, and database indexes."""

from dataclasses import dataclass, field

import oracledb

from oracle_parquet.logging_config import get_logger

logger = get_logger("db.metadata")


@dataclass(frozen=True)
class IndexColumnMetadata:
    """Detailed definition of a column within an index."""

    column_name: str
    column_position: int
    descend: str = "ASC"


@dataclass(frozen=True)
class TableIndexMetadata:
    """Metadata describing a database index on a table or view."""

    index_name: str
    uniqueness: str  # "UNIQUE" or "NONUNIQUE"
    index_type: str  # "NORMAL", "BITMAP", etc.
    is_primary_key: bool
    columns: list[IndexColumnMetadata] = field(default_factory=list)

    @property
    def column_names(self) -> list[str]:
        return [c.column_name for c in self.columns]


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
    is_primary_key: bool = False
    is_indexed: bool = False
    index_names: tuple[str, ...] = ()

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

    @property
    def key_badge(self) -> str:
        """Visual indicator of Primary Key or Database Index status."""
        if self.is_primary_key:
            return "🔑 Primary Key"
        if self.is_indexed:
            return f"⚡ Indexed ({len(self.index_names)})"
        return "—"


@dataclass(frozen=True)
class TableMetadata:
    """Metadata for an accessible Oracle table or view."""

    schema: str
    name: str
    object_type: str  # "TABLE" or "VIEW"
    num_rows: int | None = None


class OracleMetadataService:
    """Discovers accessible schemas, tables, views, column attributes, and database indexes."""

    @staticmethod
    def list_schemas(conn: oracledb.Connection) -> list[str]:
        """Discover database schemas accessible to the current user."""
        sql = """
            SELECT USERNAME AS OWNER
            FROM ALL_USERS
            ORDER BY USERNAME
        """
        schemas: list[str] = []
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql)
                for (owner,) in cursor:
                    schemas.append(owner)
            except Exception:
                cursor.execute("SELECT DISTINCT OWNER FROM ALL_TABLES ORDER BY OWNER")
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

    @classmethod
    def get_table_indexes(cls, conn: oracledb.Connection, schema: str, table: str) -> list[TableIndexMetadata]:
        """Discover database indexes and primary key constraints for the specified table."""
        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()

        # 1. Discover Primary Key constraint name and columns
        pk_constraint_names: set[str] = set()
        pk_columns: set[str] = set()
        pk_sql = """
            SELECT c.CONSTRAINT_NAME, cc.COLUMN_NAME
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc
              ON c.OWNER = cc.OWNER
             AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            WHERE c.OWNER = :schema
              AND c.TABLE_NAME = :table_name
              AND c.CONSTRAINT_TYPE = 'P'
            ORDER BY cc.POSITION
        """
        with conn.cursor() as cur:
            try:
                cur.execute(pk_sql, {"schema": schema_clean, "table_name": table_clean})
                for c_name, col_name in cur.fetchall():
                    pk_constraint_names.add(c_name.upper())
                    pk_columns.add(col_name.upper())
            except Exception as e:
                logger.warning("Could not query primary key constraints for %s.%s: %s", schema, table, e)

        # 2. Discover Indexes
        indexes: list[TableIndexMetadata] = []
        idx_sql = """
            SELECT INDEX_NAME, UNIQUENESS, INDEX_TYPE
            FROM ALL_INDEXES
            WHERE TABLE_OWNER = :schema
              AND TABLE_NAME = :table_name
            ORDER BY INDEX_NAME
        """
        col_sql = """
            SELECT COLUMN_NAME, COLUMN_POSITION, DESCEND
            FROM ALL_IND_COLUMNS
            WHERE INDEX_OWNER = :schema
              AND INDEX_NAME = :index_name
            ORDER BY COLUMN_POSITION
        """

        with conn.cursor() as cur:
            try:
                cur.execute(idx_sql, {"schema": schema_clean, "table_name": table_clean})
                idx_rows = cur.fetchall()

                for idx_name, uniqueness, idx_type in idx_rows:
                    is_pk = (idx_name.upper() in pk_constraint_names) or (
                        uniqueness == "UNIQUE" and any(idx_name.upper().endswith(s) for s in ["_PK", "PK"])
                    )

                    with conn.cursor() as col_cur:
                        col_cur.execute(col_sql, {"schema": schema_clean, "index_name": idx_name})
                        ind_cols = [
                            IndexColumnMetadata(
                                column_name=c_name,
                                column_position=pos,
                                descend=desc or "ASC",
                            )
                            for c_name, pos, desc in col_cur.fetchall()
                        ]

                    indexes.append(
                        TableIndexMetadata(
                            index_name=idx_name,
                            uniqueness=uniqueness,
                            index_type=idx_type,
                            is_primary_key=is_pk,
                            columns=ind_cols,
                        )
                    )
            except Exception as e:
                logger.warning("Could not query table indexes for %s.%s: %s", schema, table, e)

        logger.info("Discovered %d database indexes on %s.%s", len(indexes), schema_clean, table_clean)
        return indexes

    @classmethod
    def get_columns(cls, conn: oracledb.Connection, schema: str, table: str) -> list[ColumnMetadata]:
        """Retrieve column definitions, enriched with Primary Key and Index status."""
        schema_clean = schema.upper()
        table_clean = table.upper()

        # Discover indexes on table to enrich column metadata
        indexes = cls.get_table_indexes(conn, schema_clean, table_clean)
        col_to_indexes: dict[str, list[str]] = {}
        pk_cols: set[str] = set()

        for idx in indexes:
            for ic in idx.columns:
                col_upper = ic.column_name.upper()
                col_to_indexes.setdefault(col_upper, []).append(idx.index_name)
                if idx.is_primary_key:
                    pk_cols.add(col_upper)

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
            cursor.execute(sql, {"schema": schema_clean, "table_name": table_clean})
            for col_name, dtype, length, precision, scale, nullable, col_id in cursor:
                c_upper = col_name.upper()
                idx_names = tuple(col_to_indexes.get(c_upper, []))
                is_pk = c_upper in pk_cols

                columns.append(
                    ColumnMetadata(
                        name=col_name,
                        data_type=dtype,
                        data_length=length,
                        data_precision=precision,
                        data_scale=scale,
                        nullable=(nullable == "Y"),
                        column_id=col_id or (len(columns) + 1),
                        is_primary_key=is_pk,
                        is_indexed=bool(idx_names),
                        index_names=idx_names,
                    )
                )

        logger.debug("Discovered %d columns for %s.%s", len(columns), schema, table)
        return columns
