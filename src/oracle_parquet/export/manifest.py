"""Export manifest generation containing metadata, checksums, and validation results."""

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from oracle_parquet import __version__
from oracle_parquet.export.parquet_writer import ExportFileSummary
from oracle_parquet.extraction.types import ConversionWarning
from oracle_parquet.logging_config import get_logger

logger = get_logger("export.manifest")


@dataclass(frozen=True)
class ExportFileInfo:
    """Metadata describing a generated parquet file within the manifest."""

    file_name: str
    file_size_bytes: int
    row_count: int
    sha256_checksum: str


@dataclass(frozen=True)
class ExportManifest:
    """Self-describing export manifest model."""

    job_id: str
    job_status: str
    extraction_timestamp_utc: str
    source_database_service: str
    source_schema: str
    source_table: str
    selected_columns: list[str]
    source_column_types: dict[str, str]
    output_arrow_schema: list[dict[str, str]]
    row_count: int
    number_of_files: int
    compression_type: str
    files: list[ExportFileInfo]
    validation_results: dict[str, Any]
    conversion_warnings: list[dict[str, str]]
    extraction_duration_seconds: float
    database_indexes: list[dict[str, Any]] = field(default_factory=list)
    application_version: str = __version__

    def to_json(self, indent: int = 2) -> str:
        """Serialize manifest to formatted JSON."""
        return json.dumps(asdict(self), indent=indent, default=str)


class ManifestGenerator:
    """Creates and writes standardized manifest.json for completed exports."""

    @classmethod
    def create_and_save_manifest(
        cls,
        output_directory: Path,
        job_id: str,
        job_status: str,
        source_database_service: str,
        source_schema: str,
        source_table: str,
        selected_columns: list[str],
        source_column_types: dict[str, str],
        arrow_fields: list[dict[str, str]],
        file_summaries: list[ExportFileSummary],
        validation_results: dict[str, Any],
        conversion_warnings: list[ConversionWarning],
        duration_seconds: float,
        compression_type: str,
        database_indexes: list[dict[str, Any]] | None = None,
    ) -> Path:
        """Construct manifest dataclass, serialize to manifest.json, and write to disk."""
        target_dir = Path(output_directory)
        manifest_path = target_dir / "manifest.json"

        files_info = [
            ExportFileInfo(
                file_name=fs.file_name,
                file_size_bytes=fs.size_bytes,
                row_count=fs.row_count,
                sha256_checksum=fs.sha256_checksum,
            )
            for fs in file_summaries
        ]

        total_rows = sum(fs.row_count for fs in file_summaries)

        warnings_dict = [
            {
                "column_name": w.column_name,
                "oracle_type": w.oracle_type,
                "arrow_type": w.arrow_type,
                "message": w.message,
            }
            for w in conversion_warnings
        ]

        manifest = ExportManifest(
            job_id=job_id,
            job_status=job_status,
            extraction_timestamp_utc=datetime.now(UTC).isoformat(),
            source_database_service=source_database_service,
            source_schema=source_schema,
            source_table=source_table,
            selected_columns=selected_columns,
            source_column_types=source_column_types,
            output_arrow_schema=arrow_fields,
            row_count=total_rows,
            number_of_files=len(files_info),
            compression_type=compression_type,
            files=files_info,
            validation_results=validation_results,
            conversion_warnings=warnings_dict,
            extraction_duration_seconds=round(duration_seconds, 3),
            database_indexes=database_indexes or [],
        )

        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(manifest.to_json())

        logger.info("Saved export manifest to %s", manifest_path)
        return manifest_path
