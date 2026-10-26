"""Unit tests for ExportValidator, manifest integrity, and corruption detection."""

from pathlib import Path

import pyarrow as pa

from oracle_parquet.export.manifest import ManifestGenerator
from oracle_parquet.export.parquet_writer import ExportStrategy, ParquetExportWriter
from oracle_parquet.export.validation import ExportValidator, ValidationStatus


def setup_test_export(base_dir: Path, row_count: int = 200):
    schema = pa.schema(
        [
            pa.field("EMP_ID", pa.int64(), nullable=False),
            pa.field("NAME", pa.string(), nullable=True),
        ]
    )
    job_dir = base_dir / "job_test_01"
    writer = ParquetExportWriter(
        output_directory=job_dir,
        schema=schema,
        strategy=ExportStrategy.SINGLE_FILE,
    )
    batch = pa.RecordBatch.from_arrays(
        [pa.array(list(range(row_count))), pa.array([f"Emp_{i}" for i in range(row_count)])],
        schema=schema,
    )
    writer.write_batch(batch)
    summaries = writer.finish()

    arrow_fields = [{"name": f.name, "type": str(f.type), "nullable": str(f.nullable)} for f in schema]
    manifest_path = ManifestGenerator.create_and_save_manifest(
        output_directory=job_dir,
        job_id="job_test_01",
        job_status="COMPLETED",
        source_database_service="test_db",
        source_schema="HR",
        source_table="EMPLOYEES",
        selected_columns=["EMP_ID", "NAME"],
        source_column_types={"EMP_ID": "NUMBER(10)", "NAME": "VARCHAR2(50)"},
        arrow_fields=arrow_fields,
        file_summaries=summaries,
        validation_results={},
        conversion_warnings=[],
        duration_seconds=0.5,
        compression_type="zstd",
    )

    return job_dir, schema, summaries, manifest_path


def test_validation_passes_on_valid_export(tmp_path: Path):
    job_dir, schema, summaries, _ = setup_test_export(tmp_path, row_count=200)

    val = ExportValidator.validate_export(
        export_directory=job_dir,
        expected_rows=200,
        expected_schema=schema,
        expected_columns=["EMP_ID", "NAME"],
    )

    assert val.status == ValidationStatus.PASSED
    assert val.total_rows_validated == 200
    assert len(val.errors) == 0
    assert val.checks["checksums_match"] is True
    assert val.checks["row_count_match"] is True


def test_validation_fails_on_row_count_mismatch(tmp_path: Path):
    job_dir, schema, _, _ = setup_test_export(tmp_path, row_count=200)

    # Expecting 300 rows when only 200 were written
    val = ExportValidator.validate_export(
        export_directory=job_dir,
        expected_rows=300,
        expected_schema=schema,
        expected_columns=["EMP_ID", "NAME"],
    )

    assert val.status == ValidationStatus.FAILED
    assert val.checks["row_count_match"] is False
    assert any("row count" in e for e in val.errors)


def test_validation_fails_on_tampered_file_checksum(tmp_path: Path):
    job_dir, schema, summaries, _ = setup_test_export(tmp_path, row_count=200)

    # Tamper with the parquet file by appending bytes
    parquet_file = summaries[0].file_path
    with open(parquet_file, "ab") as f:
        f.write(b"corrupting_tamper_bytes")

    val = ExportValidator.validate_export(
        export_directory=job_dir,
        expected_rows=200,
        expected_schema=schema,
        expected_columns=["EMP_ID", "NAME"],
    )

    assert val.status == ValidationStatus.FAILED
    assert val.checks["checksums_match"] is False
    assert any("checksum mismatch" in e for e in val.errors)


def test_validation_fails_on_missing_manifest(tmp_path: Path):
    job_dir, schema, _, manifest_path = setup_test_export(tmp_path, row_count=100)
    manifest_path.unlink()  # delete manifest

    val = ExportValidator.validate_export(
        export_directory=job_dir,
        expected_rows=100,
        expected_schema=schema,
        expected_columns=["EMP_ID", "NAME"],
    )

    assert val.status == ValidationStatus.FAILED
    assert val.checks["manifest_exists"] is False
