"""Bounded-memory batched reader streaming data from Oracle into PyArrow RecordBatches."""

import time
from collections.abc import Callable, Generator
from dataclasses import dataclass

import pyarrow as pa
from sqlalchemy import text

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
        order_by: list[str] | str | None = None,
    ) -> None:
        self.connection_manager = connection_manager
        self.schema = schema.strip().upper()
        self.table = table.strip().upper()
        self.columns = columns
        self.batch_size = max(100, batch_size)
        self.row_limit = row_limit
        self.progress_callback = progress_callback
        self.order_by = order_by
        self.effective_order_by: list[str] | str | None = None

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

            # Auto-detect Primary Key for deterministic row-wise ordering if not specified
            effective_order_by = self.order_by
            if not effective_order_by:
                pk_cols = [c.name for c in selected_metadata if c.is_primary_key]
                if pk_cols:
                    effective_order_by = pk_cols
                    logger.info("Auto-selected Primary Key %s for deterministic row-wise ordering", effective_order_by)
            self.effective_order_by = effective_order_by

            # 3. Build safe query
            sql, params = SafeQueryBuilder.build_select_query(
                schema=self.schema,
                table=self.table,
                columns=[col.name for col in selected_metadata],
                limit=self.row_limit,
                valid_columns=[col.name for col in all_cols],
                order_by=self.effective_order_by,
            )
            logger.info("Executing extraction SQL via SQLAlchemy: %s with params %s", sql, params)

            stmt = text(sql).execution_options(yield_per=self.batch_size)
            result = conn.execute(stmt, params)

            # Fetch and yield in bounded batches via SQLAlchemy Result
            while True:
                rows = result.fetchmany(self.batch_size)
                if not rows:
                    break

                # Convert Row objects to raw tuples
                tuple_rows = [tuple(r) for r in rows]

                # Convert rows to Arrow RecordBatch
                batch = self._convert_rows_to_batch(tuple_rows, arrow_schema, selected_metadata)
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
        num_rows = len(rows)
        num_cols = len(columns_meta)
        columns_data: list[list] = [[] for _ in range(num_cols)]

        for row in rows:
            for i in range(num_cols):
                val = row[i]
                arrow_type = schema.field(i).type
                sanitized = OracleTypeMapper.sanitize_value_for_arrow(val, arrow_type)
                columns_data[i].append(sanitized)

        # Build PyArrow arrays with explicit type enforcement and row-length assertion
        arrow_arrays: list[pa.Array] = []
        for i, col_data in enumerate(columns_data):
            field = schema.field(i)
            if len(col_data) != num_rows:
                raise ValueError(
                    f"Row-wise alignment error in column '{field.name}': expected {num_rows} elements, got {len(col_data)}"
                )
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
            if len(arr) != num_rows:
                raise ValueError(
                    f"Arrow array length mismatch for column '{field.name}': expected {num_rows}, got {len(arr)}"
                )
            arrow_arrays.append(arr)

        return pa.RecordBatch.from_arrays(arrow_arrays, schema=schema)
