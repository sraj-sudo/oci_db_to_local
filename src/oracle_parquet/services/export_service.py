"""Orchestration service coordinating extraction, streaming parquet export, and validation."""

import time
import uuid
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from oracle_parquet.config import Settings, get_settings
from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.export.manifest import ManifestGenerator
from oracle_parquet.export.parquet_writer import (
    ExportFileSummary,
    ExportStrategy,
    ParquetExportWriter,
)
from oracle_parquet.export.validation import ExportValidator, ValidationResult, ValidationStatus
from oracle_parquet.extraction.reader import OracleBatchReader
from oracle_parquet.extraction.types import ConversionWarning
from oracle_parquet.logging_config import current_job_id, get_logger

logger = get_logger("services.export_service")


@dataclass(frozen=True)
class ExportJobResult:
    """Structured result returned upon completion of an export job."""

    job_id: str
    status: str  # "COMPLETED", "FAILED", "WARNING"
    schema_name: str
    table_name: str
    selected_columns: list[str]
    total_rows: int
    files: list[ExportFileSummary]
    total_bytes: int
    duration_seconds: float
    output_directory: Path
    manifest_path: Path
    validation: ValidationResult
    conversion_warnings: list[ConversionWarning]
    zip_path: Path | None = None
    error_message: str | None = None


class ExportService:
    """Orchestrates extraction, writing, manifest generation, and validation."""

    def __init__(
        self,
        connection_manager: OracleConnectionManager | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.connection_manager = connection_manager or OracleConnectionManager(self.settings)

    def run_export(
        self,
        schema: str,
        table: str,
        columns: list[str],
        row_limit: int | None = None,
        batch_size: int | None = None,
        compression: str | None = None,
        strategy: ExportStrategy = ExportStrategy.SINGLE_FILE,
        max_rows_per_file: int = 100_000,
        output_base_dir: Path | None = None,
        create_zip: bool = False,
        order_by: list[str] | str | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> ExportJobResult:
        """Execute end-to-end export workflow with strict error handling and validation."""
        start_time = time.perf_counter()
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        token = current_job_id.set(job_id)

        actual_batch_size = batch_size or self.settings.batch_size
        actual_compression = compression or self.settings.parquet_compression
        base_dir = Path(output_base_dir or self.settings.export_dir)
        job_dir = base_dir / job_id
        job_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Starting export job %s for %s.%s (strategy: %s, compression: %s, batch_size: %d)",
            job_id,
            schema,
            table,
            strategy.value,
            actual_compression,
            actual_batch_size,
        )

        writer: ParquetExportWriter | None = None
        arrow_schema = None
        warnings: list[ConversionWarning] = []
        file_summaries: list[ExportFileSummary] = []
        total_rows = 0
        db_indexes_dict: list[dict] = []

        try:
            # Discover database indexes for manifest lineage
            with self.connection_manager.get_connection() as conn:
                from oracle_parquet.db.metadata import OracleMetadataService

                tbl_indexes = OracleMetadataService.get_table_indexes(conn, schema, table)
                db_indexes_dict = [
                    {
                        "index_name": idx.index_name,
                        "uniqueness": idx.uniqueness,
                        "index_type": idx.index_type,
                        "is_primary_key": idx.is_primary_key,
                        "columns": [
                            {"column_name": ic.column_name, "position": ic.column_position, "descend": ic.descend}
                            for ic in idx.columns
                        ],
                    }
                    for idx in tbl_indexes
                ]

            reader = OracleBatchReader(
                connection_manager=self.connection_manager,
                schema=schema,
                table=table,
                columns=columns,
                batch_size=actual_batch_size,
                row_limit=row_limit,
                order_by=order_by,
                progress_callback=lambda r, b: (
                    progress_callback(r, b, f"Extracting batch {b} ({r:,} rows)") if progress_callback else None
                ),
            )

            # Iterate through bounded-memory streaming batches
            batch_gen = reader.read_batches()
            for batch, schema_ref, batch_warnings in batch_gen:
                if writer is None:
                    arrow_schema = schema_ref
                    warnings = batch_warnings
                    writer = ParquetExportWriter(
                        output_directory=job_dir,
                        schema=arrow_schema,
                        compression=actual_compression,
                        strategy=strategy,
                        max_rows_per_file=max_rows_per_file,
                    )

                writer.write_batch(batch)
                total_rows += batch.num_rows

            # If table was empty and loop never executed, finish writer with discovered schema
            if writer is None:
                # Obtain schema directly via reader discovery
                with self.connection_manager.get_connection() as conn:
                    from oracle_parquet.db.metadata import OracleMetadataService
                    from oracle_parquet.extraction.types import OracleTypeMapper

                    all_cols = OracleMetadataService.get_columns(conn, schema.upper(), table.upper())
                    col_map = {c.name.upper(): c for c in all_cols}
                    selected_meta = [col_map[c.upper()] for c in columns if c.upper() in col_map]
                    arrow_schema, warnings = OracleTypeMapper.build_arrow_schema(selected_meta)

                writer = ParquetExportWriter(
                    output_directory=job_dir,
                    schema=arrow_schema,
                    compression=actual_compression,
                    strategy=strategy,
                    max_rows_per_file=max_rows_per_file,
                )

            # Finalize writer output
            if progress_callback:
                progress_callback(total_rows, 0, "Finalizing Parquet files...")
            file_summaries = writer.finish()

            # Format Arrow schema for manifest
            arrow_fields = [{"name": f.name, "type": str(f.type), "nullable": str(f.nullable)} for f in arrow_schema]

            # Discover column types for manifest
            col_types_dict = {f.name: str(f.type) for f in arrow_schema}

            duration = time.perf_counter() - start_time

            # Initial manifest creation before validation
            manifest_path = ManifestGenerator.create_and_save_manifest(
                output_directory=job_dir,
                job_id=job_id,
                job_status="PENDING_VALIDATION",
                source_database_service=self.settings.safe_dsn,
                source_schema=schema.upper(),
                source_table=table.upper(),
                selected_columns=columns,
                source_column_types=col_types_dict,
                arrow_fields=arrow_fields,
                file_summaries=file_summaries,
                validation_results={},
                conversion_warnings=warnings,
                duration_seconds=duration,
                compression_type=actual_compression,
                database_indexes=db_indexes_dict,
            )

            # Run comprehensive validation
            if progress_callback:
                progress_callback(total_rows, 0, "Validating exported files, row-wise integrity, and checksums...")
            validation = ExportValidator.validate_export(
                export_directory=job_dir,
                expected_rows=total_rows,
                expected_schema=arrow_schema,
                expected_columns=columns,
                order_by=reader.effective_order_by if reader else order_by,
            )

            final_status = "COMPLETED" if validation.status == ValidationStatus.PASSED else validation.status.value

            # Update manifest with validation results and final status
            ManifestGenerator.create_and_save_manifest(
                output_directory=job_dir,
                job_id=job_id,
                job_status=final_status,
                source_database_service=self.settings.safe_dsn,
                source_schema=schema.upper(),
                source_table=table.upper(),
                selected_columns=columns,
                source_column_types=col_types_dict,
                arrow_fields=arrow_fields,
                file_summaries=file_summaries,
                validation_results=validation.to_dict(),
                conversion_warnings=warnings,
                duration_seconds=duration,
                compression_type=actual_compression,
                database_indexes=db_indexes_dict,
            )

            # Create ZIP bundle if requested
            zip_path: Path | None = None
            if create_zip and file_summaries:
                if progress_callback:
                    progress_callback(total_rows, 0, "Generating ZIP archive...")
                zip_path = self.create_zip_archive(job_dir, job_id)

            total_bytes = sum(f.size_bytes for f in file_summaries)
            logger.info(
                "Export job %s finished with status %s: %d rows, %d files, %d bytes in %.2fs",
                job_id,
                final_status,
                total_rows,
                len(file_summaries),
                total_bytes,
                duration,
            )

            return ExportJobResult(
                job_id=job_id,
                status=final_status,
                schema_name=schema.upper(),
                table_name=table.upper(),
                selected_columns=columns,
                total_rows=total_rows,
                files=file_summaries,
                total_bytes=total_bytes,
                duration_seconds=duration,
                output_directory=job_dir,
                manifest_path=manifest_path,
                validation=validation,
                conversion_warnings=warnings,
                zip_path=zip_path,
            )

        except Exception as exc:
            duration = time.perf_counter() - start_time
            logger.error("Export job %s failed: %s", job_id, exc, exc_info=True)

            val_fail = ValidationResult(
                status=ValidationStatus.FAILED,
                errors=[f"Job terminated with error: {exc}"],
            )

            return ExportJobResult(
                job_id=job_id,
                status="FAILED",
                schema_name=schema.upper(),
                table_name=table.upper(),
                selected_columns=columns,
                total_rows=total_rows,
                files=file_summaries,
                total_bytes=0,
                duration_seconds=duration,
                output_directory=job_dir,
                manifest_path=job_dir / "manifest.json",
                validation=val_fail,
                conversion_warnings=warnings,
                error_message=str(exc),
            )
        finally:
            current_job_id.reset(token)

    @staticmethod
    def create_zip_archive(job_dir: Path, job_id: str) -> Path:
        """Create a single zip archive of all parquet files and manifest.json."""
        zip_path = job_dir / f"{job_id}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for item in job_dir.glob("*.parquet"):
                zf.write(item, arcname=item.name)
            manifest = job_dir / "manifest.json"
            if manifest.exists():
                zf.write(manifest, arcname=manifest.name)

        return zip_path
