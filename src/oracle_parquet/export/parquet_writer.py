"""Streaming Parquet file writer supporting single and multi-file batched export strategies."""

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oracle_parquet.logging_config import get_logger

logger = get_logger("export.parquet_writer")


class ExportStrategy(StrEnum):
    """File partitioning strategy for parquet exports."""

    SINGLE_FILE = "single_file"
    MULTI_FILE = "multi_file"


@dataclass(frozen=True)
class ExportFileSummary:
    """Summary metrics for an individual generated Parquet file."""

    file_name: str
    file_path: Path
    row_count: int
    size_bytes: int
    sha256_checksum: str


class ParquetExportWriter:
    """Writes Arrow RecordBatches incrementally to one or more Parquet files."""

    def __init__(
        self,
        output_directory: Path,
        schema: pa.Schema,
        compression: str = "zstd",
        strategy: ExportStrategy = ExportStrategy.SINGLE_FILE,
        max_rows_per_file: int = 100_000,
    ) -> None:
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.compression = compression.lower()
        self.strategy = strategy
        self.max_rows_per_file = max(100, max_rows_per_file)

        self._current_file_idx = 0
        self._current_file_rows = 0
        self._current_writer: pq.ParquetWriter | None = None
        self._current_file_path: Path | None = None

        self._completed_files: list[ExportFileSummary] = []
        self._total_rows = 0

    def _get_part_file_path(self, index: int) -> Path:
        """Generate standardized part filename: part-00000.parquet."""
        return self.output_directory / f"part-{index:05d}.parquet"

    def _open_writer(self, file_path: Path) -> pq.ParquetWriter:
        """Instantiate PyArrow streaming ParquetWriter with configured compression."""
        logger.debug("Opening new Parquet writer for file: %s", file_path)
        return pq.ParquetWriter(
            where=str(file_path),
            schema=self.schema,
            compression=self.compression if self.compression != "none" else None,
            use_dictionary=True,
            write_statistics=True,
        )

    def _close_current_writer(self) -> None:
        """Close active writer, calculate file metrics and SHA-256 hash."""
        if self._current_writer is not None:
            self._current_writer.close()
            self._current_writer = None

            if self._current_file_path and self._current_file_path.exists():
                file_size = self._current_file_path.stat().st_size
                checksum = self._compute_sha256(self._current_file_path)

                summary = ExportFileSummary(
                    file_name=self._current_file_path.name,
                    file_path=self._current_file_path,
                    row_count=self._current_file_rows,
                    size_bytes=file_size,
                    sha256_checksum=checksum,
                )
                self._completed_files.append(summary)
                logger.info(
                    "Closed parquet file %s (%d rows, %d bytes, sha256=%s)",
                    summary.file_name,
                    summary.row_count,
                    summary.size_bytes,
                    summary.sha256_checksum[:12],
                )

    def write_batch(self, batch: pa.RecordBatch) -> None:
        """Write a RecordBatch incrementally, rotating files if multi-file threshold is reached."""
        batch_rows = batch.num_rows

        if self.strategy == ExportStrategy.SINGLE_FILE:
            if self._current_writer is None:
                self._current_file_path = self._get_part_file_path(0)
                self._current_writer = self._open_writer(self._current_file_path)

            self._current_writer.write_batch(batch)
            self._current_file_rows += batch_rows
            self._total_rows += batch_rows

        elif self.strategy == ExportStrategy.MULTI_FILE:
            # Check if rotation is required
            if self._current_writer is None or (self._current_file_rows >= self.max_rows_per_file and batch_rows > 0):
                if self._current_writer is not None:
                    self._close_current_writer()
                    self._current_file_idx += 1
                    self._current_file_rows = 0

                self._current_file_path = self._get_part_file_path(self._current_file_idx)
                self._current_writer = self._open_writer(self._current_file_path)

            self._current_writer.write_batch(batch)
            self._current_file_rows += batch_rows
            self._total_rows += batch_rows

    def finish(self) -> list[ExportFileSummary]:
        """Finalize writing. If no rows were written, generate an empty valid parquet file."""
        if self._total_rows == 0 and not self._completed_files and self._current_writer is None:
            # Produce an empty part-00000.parquet with proper schema
            empty_path = self._get_part_file_path(0)
            with self._open_writer(empty_path) as writer:
                # writing empty table
                empty_batch = pa.RecordBatch.from_arrays(
                    [pa.array([], type=f.type) for f in self.schema],
                    schema=self.schema,
                )
                writer.write_batch(empty_batch)

            size = empty_path.stat().st_size
            checksum = self._compute_sha256(empty_path)
            self._completed_files.append(
                ExportFileSummary(
                    file_name=empty_path.name,
                    file_path=empty_path,
                    row_count=0,
                    size_bytes=size,
                    sha256_checksum=checksum,
                )
            )
        else:
            self._close_current_writer()

        return self._completed_files

    @staticmethod
    def _compute_sha256(file_path: Path) -> str:
        """Compute SHA-256 checksum of a file in streaming chunks."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()
