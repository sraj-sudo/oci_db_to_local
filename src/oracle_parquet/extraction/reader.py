"""Bounded-memory batched reader streaming data from Oracle into PyArrow RecordBatches."""

import time
from collections.abc import Callable, Generator
from dataclasses import dataclass

import oracledb
import pyarrow as pa

from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.db.metadata import ColumnMetadata, OracleMetadataService
from oracle_parquet.db.queries import SafeQueryBuilder
from oracle_parquet.extraction.types import ConversionWarning, OracleTypeMapper
from oracle_parquet.logging_config import get_logger

logger = get_logger("extraction.reader")


@dataclass(frozen=True)
class BatchExtractionResult:
    """Summary of batched extraction process."""

    schema_name: str
    table_name: str
    selected_columns: list[str]
    total_rows_extracted: int
    arrow_schema: pa.Schema
    conversion_warnings: list[ConversionWarning]
    extraction_duration_seconds: float


class OracleBatchReader:
    """Extracts Oracle records in bounded memory batches and yields PyArrow RecordBatches."""

    def __init__(
        self,
        connection_manager: OracleConnectionManager,
        schema: str,
        table: str,
        columns: list[str],
        batch_size: int = 50000,
        row_limit: int | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> None:
        self.connection_manager = connection_manager
        self.schema = schema.strip().upper()
        self.table = table.strip().upper()
        self.columns = columns
        self.batch_size = max(100, batch_size)
        self.row_limit = row_limit
        self.progress_callback = progress_callback

    def read_batches(
        self,
    ) -> Generator[tuple[pa.RecordBatch, pa.Schema, list[ConversionWarning]], None, BatchExtractionResult]:
        """Execute query and stream PyArrow RecordBatches in bounded-memory chunks.

        Yields:
            Tuple of (batch: pa.RecordBatch, schema: pa.Schema, warnings: list[ConversionWarning]).

        Returns:
            BatchExtractionResult upon generator completion.
        """
        start_time = time.perf_counter()
        total_rows = 0
        batch_index = 0

        with self.connection_manager.get_connection() as conn:
            # Optimize driver behavior for streaming LOBs and memory
            conn.outputtypehandler = self._lob_output_type_handler

            # 1. Discover and validate columns against metadata
            all_cols = OracleMetadataService.get_columns(conn, self.schema, self.table)
            if not all_cols:
                raise ValueError(
                    f"Table or view '{self.schema}.{self.table}' has no accessible columns or does not exist."
                )

            col_map = {c.name.upper(): c for c in all_cols}
            selected_metadata: list[ColumnMetadata] = []

            for col_name in self.columns:
                c_upper = col_name.strip().upper()
                if c_upper not in col_map:
                    raise ValueError(f"Selected column '{col_name}' does not exist in '{self.schema}.{self.table}'")
                selected_metadata.append(col_map[c_upper])

            # 2. Build Arrow Schema
            arrow_schema, warnings = OracleTypeMapper.build_arrow_schema(selected_metadata)

            # 3. Build safe query
            sql, params = SafeQueryBuilder.build_select_query(
                schema=self.schema,
                table=self.table,
                columns=[col.name for col in selected_metadata],
                limit=self.row_limit,
                valid_columns=[col.name for col in all_cols],
            )
            logger.info("Executing extraction SQL: %s with params %s", sql, params)

            with conn.cursor() as cursor:
                cursor.arraysize = self.batch_size
                cursor.execute(sql, params)

                # Fetch and yield in bounded batches
                while True:
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    # Convert rows to Arrow RecordBatch
                    batch = self._convert_rows_to_batch(rows, arrow_schema, selected_metadata)
                    total_rows += len(rows)
                    batch_index += 1

                    if self.progress_callback:
                        self.progress_callback(total_rows, batch_index)

                    yield batch, arrow_schema, warnings

        duration = time.perf_counter() - start_time
        logger.info(
            "Extraction completed for %s.%s: %d total rows across %d batches in %.2fs",
            self.schema,
            self.table,
            total_rows,
            batch_index,
            duration,
        )

        return BatchExtractionResult(
            schema_name=self.schema,
            table_name=self.table,
            selected_columns=[col.name for col in selected_metadata],
            total_rows_extracted=total_rows,
            arrow_schema=arrow_schema,
            conversion_warnings=warnings,
            extraction_duration_seconds=duration,
        )

    def _convert_rows_to_batch(
        self,
        rows: list[tuple],
        schema: pa.Schema,
        columns_meta: list[ColumnMetadata],
    ) -> pa.RecordBatch:
        """Convert a list of tuple rows into a typed PyArrow RecordBatch."""
        num_cols = len(columns_meta)
        columns_data: list[list] = [[] for _ in range(num_cols)]

        for row in rows:
            for i in range(num_cols):
                val = row[i]
                arrow_type = schema.field(i).type
                sanitized = OracleTypeMapper.sanitize_value_for_arrow(val, arrow_type)
                columns_data[i].append(sanitized)

        # Build PyArrow arrays with explicit type enforcement
        arrow_arrays: list[pa.Array] = []
        for i, col_data in enumerate(columns_data):
            field = schema.field(i)
            try:
                arr = pa.array(col_data, type=field.type)
            except Exception as arr_err:
                logger.warning(
                    "Error building Arrow array for column %s of type %s: %s. Attempting fallback.",
                    field.name,
                    field.type,
                    arr_err,
                )
                # Fallback to string or safe array representation
                arr = pa.array([str(x) if x is not None else None for x in col_data], type=pa.string())
            arrow_arrays.append(arr)

        return pa.RecordBatch.from_arrays(arrow_arrays, schema=schema)

    @staticmethod
    def _lob_output_type_handler(cursor, name, default_type, size, precision, scale):
        """Instruct python-oracledb to return CLOB/BLOB as Python str/bytes directly."""
        if default_type == oracledb.DB_TYPE_CLOB:
            return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
        if default_type == oracledb.DB_TYPE_BLOB:
            return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
        return None
