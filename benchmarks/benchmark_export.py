"""Reproducible benchmark measuring serialization, memory, throughput, and compression.

Benchmarks datasets with 10k, 100k, and 1,000,000 rows without fabricating performance metrics.
"""

import decimal
import json
import platform
import time
import tracemalloc
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from oracle_parquet import __version__
from oracle_parquet.export.manifest import ManifestGenerator
from oracle_parquet.export.parquet_writer import ExportStrategy, ParquetExportWriter
from oracle_parquet.export.validation import ExportValidator


def create_synthetic_schema() -> pa.Schema:
    """Create representative schema matching typical Oracle enterprise tables."""
    return pa.schema(
        [
            pa.field("ID", pa.int64(), nullable=False),
            pa.field("ACCOUNT_NUMBER", pa.string(), nullable=False),
            pa.field("TXN_AMOUNT", pa.decimal128(14, 2), nullable=True),
            pa.field("CREATED_AT", pa.timestamp("us"), nullable=False),
            pa.field("NOTES", pa.string(), nullable=True),
            pa.field("IS_ACTIVE", pa.int16(), nullable=False),
        ]
    )


def generate_synthetic_batches(
    total_rows: int,
    batch_size: int,
    schema: pa.Schema,
) -> Generator[pa.RecordBatch, None, None]:
    """Stream synthetic records in bounded-memory chunks without holding all rows in memory."""
    rows_generated = 0
    base_time = int(datetime.now(UTC).timestamp() * 1_000_000)

    while rows_generated < total_rows:
        current_batch_size = min(batch_size, total_rows - rows_generated)
        ids = list(range(rows_generated + 1, rows_generated + current_batch_size + 1))
        accounts = [f"ACC-{idx:08d}" for idx in ids]
        amounts = [decimal.Decimal(f"{(idx * 17) % 100000}.{(idx * 3) % 100:02d}") for idx in ids]
        timestamps = [base_time + idx * 1000 for idx in ids]
        notes = [f"Transaction note for entry {idx} with extra text payload" if idx % 7 != 0 else None for idx in ids]
        is_active = [1 if idx % 2 == 0 else 0 for idx in ids]

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(ids, type=pa.int64()),
                pa.array(accounts, type=pa.string()),
                pa.array(amounts, type=pa.decimal128(14, 2)),
                pa.array(timestamps, type=pa.timestamp("us")),
                pa.array(notes, type=pa.string()),
                pa.array(is_active, type=pa.int16()),
            ],
            schema=schema,
        )
        yield batch
        rows_generated += current_batch_size


def run_benchmark_for_row_count(
    row_count: int,
    batch_size: int = 50_000,
    compression: str = "zstd",
    output_dir: Path | None = None,
) -> dict:
    """Execute timed benchmark with peak memory tracing for a specific dataset size."""
    out_dir = output_dir or Path(f"data/benchmark_{row_count}")
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = create_synthetic_schema()

    tracemalloc.start()
    start_total = time.perf_counter()

    # Parquet Streaming Export
    writer = ParquetExportWriter(
        output_directory=out_dir,
        schema=schema,
        compression=compression,
        strategy=ExportStrategy.SINGLE_FILE,
    )

    uncompressed_bytes_approx = 0
    for batch in generate_synthetic_batches(row_count, batch_size, schema):
        uncompressed_bytes_approx += batch.nbytes
        writer.write_batch(batch)

    summaries = writer.finish()
    export_duration = time.perf_counter() - start_total
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    total_size = sum(s.size_bytes for s in summaries)
    throughput_rps = row_count / export_duration if export_duration > 0 else 0.0
    compression_ratio = uncompressed_bytes_approx / total_size if total_size > 0 else 1.0

    # Validation
    val_start = time.perf_counter()
    arrow_fields = [{"name": f.name, "type": str(f.type), "nullable": str(f.nullable)} for f in schema]
    ManifestGenerator.create_and_save_manifest(
        output_directory=out_dir,
        job_id=f"bench_{row_count}",
        job_status="COMPLETED",
        source_database_service="SYNTHETIC_BENCHMARK",
        source_schema="SYNTHETIC",
        source_table=f"TABLE_{row_count}_ROWS",
        selected_columns=[f.name for f in schema],
        source_column_types={f.name: str(f.type) for f in schema},
        arrow_fields=arrow_fields,
        file_summaries=summaries,
        validation_results={},
        conversion_warnings=[],
        duration_seconds=export_duration,
        compression_type=compression,
    )

    validation = ExportValidator.validate_export(
        export_directory=out_dir,
        expected_rows=row_count,
        expected_schema=schema,
        expected_columns=[f.name for f in schema],
    )
    val_duration = time.perf_counter() - val_start
    total_duration = time.perf_counter() - start_total

    return {
        "row_count": row_count,
        "batch_size": batch_size,
        "compression": compression,
        "export_duration_s": round(export_duration, 4),
        "validation_duration_s": round(val_duration, 4),
        "total_duration_s": round(total_duration, 4),
        "throughput_rows_per_sec": round(throughput_rps, 1),
        "peak_memory_mb": round(peak_memory / (1024 * 1024), 2),
        "output_size_mb": round(total_size / (1024 * 1024), 2),
        "compression_ratio": round(compression_ratio, 2),
        "validation_status": validation.status.value,
        "oracle_live_extraction_measured": False,
        "note": "Oracle extraction not measured due to absence of live database; synthetic streaming serialization tested.",
    }


def main():
    """Run benchmark suite across 10k, 100k, and 1,000,000 rows."""
    print("=" * 80)
    print("ORACLE-TO-PARQUET PIPELINE - SERIALIZATION & EXPORT BENCHMARK")
    print(
        f"Python: {platform.python_version()} | OS: {platform.system()} {platform.release()} | PyArrow: {pa.__version__}"
    )
    print("=" * 80)

    row_counts = [10_000, 100_000, 1_000_000]
    results = []

    for rows in row_counts:
        print(f"\n▶ Benchmarking {rows:,} rows (batch_size=50,000, compression=zstd)...")
        res = run_benchmark_for_row_count(rows, batch_size=50_000, compression="zstd")
        results.append(res)
        print(
            f"  ✔ Completed in {res['total_duration_s']}s | "
            f"Throughput: {res['throughput_rows_per_sec']:,.0f} rows/s | "
            f"Peak RAM: {res['peak_memory_mb']} MB | "
            f"Size: {res['output_size_mb']} MB | "
            f"Ratio: {res['compression_ratio']}:1 | "
            f"Validation: {res['validation_status']}"
        )

    # Save benchmark results
    out_file = Path("benchmarks/benchmark_results.json")
    benchmark_meta = {
        "benchmark_timestamp_utc": datetime.now(UTC).isoformat(),
        "application_version": __version__,
        "system": {
            "os": platform.platform(),
            "cpu_count": platform.os.cpu_count(),
            "python_version": platform.python_version(),
            "pyarrow_version": pa.__version__,
        },
        "results": results,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(benchmark_meta, f, indent=2)

    print(f"\n[OK] Benchmark results written to {out_file}")


if __name__ == "__main__":
    main()
