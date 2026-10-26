"""Post-export verification and validation module."""

import hashlib
import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from oracle_parquet.logging_config import get_logger

logger = get_logger("export.validation")


class ValidationStatus(StrEnum):
    """Overall status of export validation check."""

    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"


@dataclass
class ValidationResult:
    """Detailed results of all validation checks performed on an export."""

    status: ValidationStatus
    total_files_checked: int = 0
    total_rows_validated: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks: dict[str, bool] = field(default_factory=dict)
    sample_rows_preview: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert validation result to dictionary suitable for JSON serialization."""
        return {
            "status": self.status.value,
            "total_files_checked": self.total_files_checked,
            "total_rows_validated": self.total_rows_validated,
            "errors": self.errors,
            "warnings": self.warnings,
            "checks": self.checks,
            "sample_rows_preview": self.sample_rows_preview,
        }


class ExportValidator:
    """Validates Parquet files, schemas, checksums, and manifests against extraction specs."""

    @classmethod
    def validate_export(
        cls,
        export_directory: Path,
        expected_rows: int,
        expected_schema: pa.Schema,
        expected_columns: list[str],
        order_by: list[str] | str | None = None,
    ) -> ValidationResult:
        """Execute comprehensive validation suite on an export directory."""
        dir_path = Path(export_directory)
        result = ValidationResult(status=ValidationStatus.PASSED)

        # Check 1: Directory exists
        if not dir_path.is_dir():
            result.errors.append(f"Export directory does not exist: {dir_path}")
            result.status = ValidationStatus.FAILED
            result.checks["directory_exists"] = False
            return result
        result.checks["directory_exists"] = True

        # Check 2: Manifest file exists and can be parsed
        manifest_path = dir_path / "manifest.json"
        if not manifest_path.exists():
            result.errors.append("manifest.json is missing in export directory")
            result.status = ValidationStatus.FAILED
            result.checks["manifest_exists"] = False
            return result

        result.checks["manifest_exists"] = True
        try:
            with open(manifest_path, encoding="utf-8") as f:
                manifest_data = json.load(f)
        except Exception as exc:
            result.errors.append(f"Failed to read or parse manifest.json: {exc}")
            result.status = ValidationStatus.FAILED
            result.checks["manifest_valid_json"] = False
            return result
        result.checks["manifest_valid_json"] = True

        # Check 3: Manifest required fields
        required_manifest_fields = [
            "job_id",
            "job_status",
            "extraction_timestamp_utc",
            "source_database_service",
            "source_schema",
            "source_table",
            "selected_columns",
            "row_count",
            "number_of_files",
            "files",
        ]
        missing_fields = [f for f in required_manifest_fields if f not in manifest_data]
        if missing_fields:
            result.errors.append(f"Manifest missing required fields: {missing_fields}")
            result.status = ValidationStatus.FAILED
            result.checks["manifest_schema"] = False
        else:
            result.checks["manifest_schema"] = True

        # Check 4: Validate each parquet file
        manifest_files = manifest_data.get("files", [])
        if not manifest_files:
            result.errors.append("Manifest specifies zero output files")
            result.status = ValidationStatus.FAILED
            result.checks["has_output_files"] = False
            return result
        result.checks["has_output_files"] = True

        actual_total_rows = 0
        result.total_files_checked = len(manifest_files)

        for file_entry in manifest_files:
            fname = file_entry.get("file_name")
            expected_size = file_entry.get("file_size_bytes")
            expected_hash = file_entry.get("sha256_checksum")
            file_entry.get("row_count", 0)

            fpath = dir_path / fname
            if not fpath.exists():
                result.errors.append(f"Expected file '{fname}' does not exist on disk")
                result.status = ValidationStatus.FAILED
                continue

            # Verify file size
            actual_size = fpath.stat().st_size
            if actual_size != expected_size:
                result.errors.append(f"File '{fname}' size mismatch: expected {expected_size}, got {actual_size}")
                result.status = ValidationStatus.FAILED

            # Verify checksum
            actual_hash = cls._compute_sha256(fpath)
            if actual_hash != expected_hash:
                result.errors.append(f"File '{fname}' checksum mismatch: expected {expected_hash}, got {actual_hash}")
                result.status = ValidationStatus.FAILED

            # Verify parquet readability and schema
            try:
                parquet_file = pq.ParquetFile(str(fpath))
                actual_schema = parquet_file.schema_arrow

                # Check column ordering
                actual_names = [f.name for f in actual_schema]
                if actual_names != expected_columns:
                    result.errors.append(
                        f"Column order in '{fname}' ({actual_names}) does not match expected ({expected_columns})"
                    )
                    result.status = ValidationStatus.FAILED

                # Check schema compatibility (field types)
                for exp_field in expected_schema:
                    try:
                        act_field = actual_schema.field(exp_field.name)
                        if act_field.type != exp_field.type:
                            result.warnings.append(
                                f"Type mismatch in '{fname}' for column {exp_field.name}: "
                                f"expected {exp_field.type}, got {act_field.type}"
                            )
                            if result.status != ValidationStatus.FAILED:
                                result.status = ValidationStatus.WARNING
                    except KeyError:
                        result.errors.append(f"Missing field '{exp_field.name}' in '{fname}'")
                        result.status = ValidationStatus.FAILED

                # Verify row counts
                actual_total_rows += parquet_file.metadata.num_rows

                # Row-wise integrity verification:
                # 1. Check all row groups to ensure every column chunk contains exactly rg.num_rows
                for rg_idx in range(parquet_file.metadata.num_row_groups):
                    rg_meta = parquet_file.metadata.row_group(rg_idx)
                    rg_rows = rg_meta.num_rows
                    for col_idx in range(rg_meta.num_columns):
                        col_chunk = rg_meta.column(col_idx)
                        if col_chunk.num_values != rg_rows:
                            result.errors.append(
                                f"Row-column alignment violation in '{fname}' row group {rg_idx}, column {col_idx}: "
                                f"expected {rg_rows} values, found {col_chunk.num_values}"
                            )
                            result.status = ValidationStatus.FAILED

                # 2. Round-trip read first record batch to ensure row-level data integrity
                if parquet_file.metadata.num_rows > 0:
                    tbl = parquet_file.read_row_group(0)
                    assert len(tbl) > 0

                    # Reconstruct row records and verify strict column key ordering
                    sample_batch = tbl.slice(0, min(50, len(tbl))).to_pylist()
                    for r_idx, row_dict in enumerate(sample_batch[:5]):
                        if list(row_dict.keys()) != expected_columns:
                            result.errors.append(
                                f"Row-wise key mismatch in '{fname}' at row {r_idx}: "
                                f"got {list(row_dict.keys())}, expected {expected_columns}"
                            )
                            result.status = ValidationStatus.FAILED
                            break

                    # Preserve first 5 rows for sample preview in manifest / UI
                    if not result.sample_rows_preview and sample_batch:
                        result.sample_rows_preview = sample_batch[:5]

                    # 3. Check deterministic ordering if order_by / primary key was used
                    if order_by:
                        sort_col = order_by if isinstance(order_by, str) else order_by[0]
                        clean_col = sort_col.strip('"').upper()
                        matching_col = next((c for c in expected_columns if c.upper() == clean_col), None)
                        if matching_col and matching_col in tbl.column_names:
                            col_vals = [v for v in tbl.column(matching_col).to_pylist() if v is not None]
                            if col_vals:
                                is_sorted = all(col_vals[i] <= col_vals[i + 1] for i in range(len(col_vals) - 1))
                                if is_sorted:
                                    result.checks["row_order_verified"] = True
                                else:
                                    result.warnings.append(
                                        f"Sort column '{matching_col}' in '{fname}' was not strictly non-decreasing"
                                    )

            except Exception as read_err:
                result.errors.append(f"Failed to read parquet file '{fname}': {read_err}")
                result.status = ValidationStatus.FAILED

        result.total_rows_validated = actual_total_rows
        result.checks["files_readable"] = not any("read parquet file" in e for e in result.errors)
        result.checks["checksums_match"] = not any("checksum mismatch" in e for e in result.errors)
        result.checks["sizes_match"] = not any("size mismatch" in e for e in result.errors)
        result.checks["row_wise_integrity"] = not any(
            "Row-column alignment" in e or "Row-wise key mismatch" in e for e in result.errors
        )

        # Check 5: Total row count matches extraction count
        if actual_total_rows != expected_rows:
            result.errors.append(
                f"Total validated row count ({actual_total_rows}) does not match expected ({expected_rows})"
            )
            result.status = ValidationStatus.FAILED
            result.checks["row_count_match"] = False
        else:
            result.checks["row_count_match"] = True

        logger.info(
            "Export validation completed with status %s (%d errors, %d warnings)",
            result.status.value,
            len(result.errors),
            len(result.warnings),
        )
        return result

    @staticmethod
    def _compute_sha256(file_path: Path) -> str:
        """Compute SHA-256 hash of a file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
