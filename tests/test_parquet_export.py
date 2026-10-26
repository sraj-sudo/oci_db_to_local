"""Unit tests for ParquetExportWriter covering single-file, multi-file, and empty streams."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oracle_parquet.export.parquet_writer import ExportStrategy, ParquetExportWriter


def make_test_batch(start_id: int, count: int) -> tuple[pa.RecordBatch, pa.Schema]:
    schema = pa.schema(
        [
            pa.field("ID", pa.int64(), nullable=False),
            pa.field("NAME", pa.string(), nullable=True),
        ]
    )
    ids = list(range(start_id, start_id + count))
    names = [f"Name_{i}" if i % 5 != 0 else None for i in ids]
    batch = pa.RecordBatch.from_arrays([pa.array(ids), pa.array(names)], schema=schema)
    return batch, schema


def test_single_file_export(tmp_path: Path):
    output_dir = tmp_path / "single_export"
    batch, schema = make_test_batch(1, 250)

    writer = ParquetExportWriter(
        output_directory=output_dir,
        schema=schema,
        compression="zstd",
        strategy=ExportStrategy.SINGLE_FILE,
    )
    writer.write_batch(batch)
    writer.write_batch(batch)  # total 500 rows
    files = writer.finish()

    assert len(files) == 1
    assert files[0].file_name == "part-00000.parquet"
    assert files[0].row_count == 500
    assert files[0].file_path.exists()
    assert len(files[0].sha256_checksum) == 64

    # Verify parquet file metadata
    pf = pq.ParquetFile(str(files[0].file_path))
    assert pf.metadata.num_rows == 500
    assert pf.schema_arrow.equals(schema)


def test_multi_file_export_rotation(tmp_path: Path):
    output_dir = tmp_path / "multi_export"
    batch, schema = make_test_batch(1, 100)

    writer = ParquetExportWriter(
        output_directory=output_dir,
        schema=schema,
        compression="snappy",
        strategy=ExportStrategy.MULTI_FILE,
        max_rows_per_file=150,
    )
    # Write 3 batches of 100 rows each -> total 300 rows -> 2 or 3 files
    writer.write_batch(batch)
    writer.write_batch(batch)
    writer.write_batch(batch)
    files = writer.finish()

    assert len(files) >= 2
    total_written = sum(f.row_count for f in files)
    assert total_written == 300
    for f in files:
        assert f.file_path.exists()
        assert f.size_bytes > 0


def test_empty_result_set_export(tmp_path: Path):
    output_dir = tmp_path / "empty_export"
    schema = pa.schema([pa.field("COL_A", pa.string()), pa.field("COL_B", pa.int32())])

    writer = ParquetExportWriter(
        output_directory=output_dir,
        schema=schema,
        strategy=ExportStrategy.SINGLE_FILE,
    )
    files = writer.finish()

    assert len(files) == 1
    assert files[0].row_count == 0
    assert files[0].file_path.exists()

    pf = pq.ParquetFile(str(files[0].file_path))
    assert pf.metadata.num_rows == 0
    assert pf.schema_arrow.names == ["COL_A", "COL_B"]
