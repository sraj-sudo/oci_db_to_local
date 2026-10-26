"""Data preview service fetching bounded sample data with type and null analysis."""

from dataclasses import dataclass

import pandas as pd

from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.db.metadata import ColumnMetadata, OracleMetadataService
from oracle_parquet.db.queries import SafeQueryBuilder
from oracle_parquet.extraction.types import ConversionWarning, OracleTypeMapper
from oracle_parquet.logging_config import get_logger

logger = get_logger("extraction.preview")


@dataclass(frozen=True)
class PreviewData:
    """Preview metrics and sample records for Streamlit display."""

    dataframe: pd.DataFrame
    column_names: list[str]
    oracle_types: dict[str, str]
    null_counts: dict[str, int]
    row_count: int
    conversion_warnings: list[ConversionWarning]


class DataPreviewService:
    """Executes a bounded sample query and gathers preview statistics."""

    def __init__(self, connection_manager: OracleConnectionManager) -> None:
        self.connection_manager = connection_manager

    def get_preview(
        self,
        schema: str,
        table: str,
        columns: list[str],
        limit: int = 100,
    ) -> PreviewData:
        """Fetch a limited sample of records and compute preview metrics."""
        schema_clean = schema.strip().upper()
        table_clean = table.strip().upper()
        limit_clean = max(1, min(limit, 10_000))

        if not columns:
            raise ValueError("At least one column must be selected for preview")

        with self.connection_manager.get_connection() as conn:
            # Discover metadata for selected columns
            all_cols = OracleMetadataService.get_columns(conn, schema_clean, table_clean)
            col_map = {c.name.upper(): c for c in all_cols}
            selected_meta: list[ColumnMetadata] = []

            for col in columns:
                c_upper = col.strip().upper()
                if c_upper not in col_map:
                    raise ValueError(f"Column '{col}' does not exist in '{schema_clean}.{table_clean}'")
                selected_meta.append(col_map[c_upper])

            # Generate type mapping and warnings
            arrow_schema, warnings = OracleTypeMapper.build_arrow_schema(selected_meta)
            oracle_types = {c.name: c.display_type for c in selected_meta}

            # Generate safe query with row limit
            sql, params = SafeQueryBuilder.build_select_query(
                schema=schema_clean,
                table=table_clean,
                columns=[c.name for c in selected_meta],
                limit=limit_clean,
                valid_columns=[c.name for c in all_cols],
            )

            from sqlalchemy import text

            stmt = text(sql)
            res = conn.execute(stmt, params)
            rows = [tuple(r) for r in res.fetchmany(limit_clean)]

        # Build DataFrame and compute null counts
        col_names = [c.name for c in selected_meta]
        if rows:
            # Convert values safely
            processed_rows = []
            for r in rows:
                processed_rows.append(
                    [
                        OracleTypeMapper.sanitize_value_for_arrow(val, arrow_schema.field(i).type)
                        for i, val in enumerate(r)
                    ]
                )
            df = pd.DataFrame(processed_rows, columns=col_names)
            null_counts = df.isnull().sum().to_dict()
        else:
            df = pd.DataFrame(columns=col_names)
            null_counts = {c: 0 for c in col_names}

        return PreviewData(
            dataframe=df,
            column_names=col_names,
            oracle_types=oracle_types,
            null_counts={k: int(v) for k, v in null_counts.items()},
            row_count=len(df),
            conversion_warnings=warnings,
        )
