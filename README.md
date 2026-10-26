# Oracle to Apache Parquet Pipeline

High-performance, bounded-memory extraction pipeline from Oracle Database to Apache Parquet with a Streamlit interface.

---

## 1. Project Architecture

The architecture enforces strict separation of concerns across database access, query construction, data extraction, type translation, Parquet writing, validation, and user interface.

```
oracle-parquet-pipeline/
├── app.py                      # Streamlit User Interface
├── pyproject.toml              # UV / Hatchling packaging & dependencies
├── uv.lock                     # Deterministic dependency lockfile
├── .env.example                # Sample environment variables
├── .gitignore                  # Git exclusions (credentials, exports, logs)
├── README.md                   # System documentation
├── src/
│   └── oracle_parquet/
│       ├── __init__.py         # Package root
│       ├── config.py           # Typed settings via pydantic-settings
│       ├── logging_config.py   # Structured logging with secret sanitization
│       ├── db/
│       │   ├── connection.py   # OracleConnectionManager (Thin & Thick modes)
│       │   ├── metadata.py     # OracleMetadataService (schemas, tables, columns)
│       │   └── queries.py      # SafeQueryBuilder (SQL injection prevention)
│       ├── extraction/
│       │   ├── preview.py      # Bounded sample preview service
│       │   ├── reader.py       # OracleBatchReader (bounded-memory stream)
│       │   └── types.py        # OracleTypeMapper (precision & type preservation)
│       ├── export/
│       │   ├── parquet_writer.py # PyArrow streaming writer (single/multi-file)
│       │   ├── manifest.py     # Self-describing manifest.json generator
│       │   └── validation.py   # Checksum, row-count & schema validator
│       └── services/
│           └── export_service.py # End-to-end headless export orchestrator
├── tests/
│   ├── conftest.py             # Pytest fixtures and mock objects
│   ├── test_config.py          # Configuration and masking tests
│   ├── test_metadata.py        # Metadata discovery and error tests
│   ├── test_query_safety.py    # SQL injection and identifier tests
│   ├── test_type_mapping.py    # Oracle to Arrow type conversion tests
│   ├── test_parquet_export.py  # Single/multi-file and empty export tests
│   ├── test_export_validation.py # Checksum and manifest validation tests
│   └── test_integration_oracle.py # Live Oracle integration test harness
├── benchmarks/
│   ├── benchmark_export.py     # Reproducible synthetic benchmark script
│   └── benchmark_results.json  # Recorded benchmark metrics
├── data/
│   └── exports/                # Export outputs partitioned by JOB_ID
└── logs/                       # Application rotating log files
```

---

## 2. Environment Setup & Dependency Installation

### Prerequisites
- Python 3.12+
- `uv` package and environment manager

### Installation
Clone or navigate to the repository and install all dependencies:

```bash
# Sync dependencies in virtual environment with dev tools
uv sync --extra dev --python 3.12
```

---

## 3. Oracle Connection Configuration

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Edit `.env` to configure your Oracle credentials and connection details:

```ini
# Oracle Database Credentials
ORACLE_USER=readonly_user
ORACLE_PASSWORD=your_secure_password
ORACLE_DSN=dbhost.company.internal:1521/XEPDB1

# Client Mode: 'thin' (default, pure Python) or 'thick' (Instant Client)
ORACLE_CLIENT_MODE=thin

# Oracle Autonomous Database / Wallet (Optional)
ORACLE_WALLET_LOCATION=/path/to/wallet_dir
ORACLE_WALLET_PASSWORD=wallet_password_if_encrypted

# Export and Extraction Defaults
EXPORT_DIR=data/exports
BATCH_SIZE=50000
PARQUET_COMPRESSION=zstd
```

### Wallet Configuration (Oracle Autonomous Database / OCI)
When connecting to Oracle Autonomous Database (ATP / ADW):
1. Download the client credentials zip (`wallet_DBNAME.zip`).
2. Unpack the files (`cwallet.sso`, `tnsnames.ora`, `sqlnet.ora`) into a secure local directory.
3. Set `ORACLE_WALLET_LOCATION=/absolute/path/to/wallet_dir`.
4. Set `ORACLE_DSN` to the TNS alias defined in `tnsnames.ora` (e.g., `dbname_high`).
5. Ensure `sqlnet.ora` points to the correct directory location.

---

## 4. Running the Streamlit Application

Launch the application using `uv`:

```bash
uv run streamlit run app.py
```

Open your browser at `http://localhost:8501`.

### User Interface Workflow
1. **Sidebar Connection Panel**: Displays connection status, configured DSN (with masked secrets), username, and driver mode. Click **Test Connection** to verify database connectivity.
2. **Tab 1: Source Selection**:
   - Discovers accessible schemas from Oracle data dictionary (`ALL_TABLES`, `ALL_VIEWS`).
   - Select schema and target table or view.
   - Choose columns using the multiselect control (preserve column ordering or use **Select All**).
3. **Tab 2: Data Preview**:
   - Inspect a bounded sample of rows (default 50-100 rows).
   - View Oracle types, null counts per column, and type conversion warnings.
4. **Tab 3: Export & Status**:
   - Configure row extraction limit (All Rows or limited rows).
   - Configure batch size (cursor arraysize and chunk size).
   - Select compression codec: **Zstandard (zstd)** default or **Snappy**.
   - Select file strategy: **Single File** or **Multi-File** (rotates files upon hitting row thresholds).
   - Click **Start Parquet Export** to run streaming extraction, Parquet generation, and post-export validation.
   - Download individual Parquet files, `manifest.json`, or a bundled ZIP archive.

---

## 5. Headless Execution Engine

The core extraction and export engine is completely independent of Streamlit and can be integrated into scheduled CLI jobs, Airflow DAGs, or microservices:

```python
from pathlib import Path
from oracle_parquet.config import get_settings
from oracle_parquet.db.connection import OracleConnectionManager
from oracle_parquet.export.parquet_writer import ExportStrategy
from oracle_parquet.services.export_service import ExportService

settings = get_settings()
conn_mgr = OracleConnectionManager(settings)
service = ExportService(connection_manager=conn_mgr, settings=settings)

result = service.run_export(
    schema="HR",
    table="EMPLOYEES",
    columns=["EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "SALARY", "HIRE_DATE"],
    row_limit=100_000,
    batch_size=50_000,
    compression="zstd",
    strategy=ExportStrategy.SINGLE_FILE,
    output_base_dir=Path("data/exports"),
    create_zip=True,
)

print(f"Status: {result.status}")
print(f"Extracted {result.total_rows:,} rows in {result.duration_seconds:.2f}s")
print(f"Manifest written to: {result.manifest_path}")
print(f"Validation Status: {result.validation.status.value}")
```

---

## 6. Output Format & Manifest Specifications

Each completed export creates a unique directory under `data/exports/JOB_ID/`:

```
data/exports/job_20261026_110000_a1b2c3/
├── part-00000.parquet
├── part-00001.parquet
├── manifest.json
└── job_20261026_110000_a1b2c3.zip
```

### Manifest Schema (`manifest.json`)
The manifest document records complete lineage and cryptographic checksums:
```json
{
  "job_id": "job_20261026_110000_a1b2c3",
  "job_status": "COMPLETED",
  "extraction_timestamp_utc": "2026-10-26T05:30:00.000000+00:00",
  "source_database_service": "dbhost:1521/XEPDB1",
  "source_schema": "HR",
  "source_table": "EMPLOYEES",
  "selected_columns": ["EMPLOYEE_ID", "SALARY", "HIRE_DATE"],
  "source_column_types": {
    "EMPLOYEE_ID": "NUMBER(6)",
    "SALARY": "NUMBER(8,2)",
    "HIRE_DATE": "DATE"
  },
  "output_arrow_schema": [
    {"name": "EMPLOYEE_ID", "type": "int32", "nullable": "False"},
    {"name": "SALARY", "type": "decimal128(8, 2)", "nullable": "True"},
    {"name": "HIRE_DATE", "type": "timestamp[us]", "nullable": "True"}
  ],
  "row_count": 107,
  "number_of_files": 1,
  "compression_type": "zstd",
  "files": [
    {
      "file_name": "part-00000.parquet",
      "file_size_bytes": 14205,
      "row_count": 107,
      "sha256_checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    }
  ],
  "validation_results": {
    "status": "PASSED",
    "total_files_checked": 1,
    "total_rows_validated": 107,
    "errors": [],
    "warnings": [],
    "checks": {
      "directory_exists": true,
      "manifest_exists": true,
      "manifest_valid_json": true,
      "manifest_schema": true,
      "has_output_files": true,
      "files_readable": true,
      "checksums_match": true,
      "sizes_match": true,
      "row_count_match": true
    }
  },
  "conversion_warnings": [],
  "extraction_duration_seconds": 0.42,
  "application_version": "0.1.0"
}
```

---

## 7. Data Type Preservation & Policies

| Oracle Data Type | PyArrow Target Type | Policy & Preservation Notes |
| :--- | :--- | :--- |
| `VARCHAR2`, `CHAR`, `NCHAR`, `NVARCHAR2` | `pa.string()` | Preserves text and identifiers with leading zeros. |
| `NUMBER(p, 0)` (`p <= 4`) | `pa.int16()` | Exact integer representation. |
| `NUMBER(p, 0)` (`5 <= p <= 9`) | `pa.int32()` | Exact integer representation. |
| `NUMBER(p, 0)` (`10 <= p <= 18`) | `pa.int64()` | Exact integer representation. |
| `NUMBER(p, s)` (`s > 0`, `p <= 38`) | `pa.decimal128(p, s)` | Exact fixed-point numeric precision; no floating-point rounding. |
| `NUMBER` (unconstrained) | `pa.float64()` | Logged with conversion warning (loss of precision possible for >15 digits). |
| `FLOAT`, `BINARY_DOUBLE` | `pa.float64()` | IEEE 754 double precision float. |
| `BINARY_FLOAT` | `pa.float32()` | IEEE 754 single precision float. |
| `DATE` | `pa.timestamp('us')` | Preserves hours, minutes, and seconds from Oracle `DATE`. |
| `TIMESTAMP` | `pa.timestamp('us')` | Preserves microsecond temporal precision. |
| `TIMESTAMP WITH TIME ZONE` | `pa.timestamp('us', tz='UTC')` | Normalized to UTC. |
| `CLOB`, `NCLOB` | `pa.large_string()` | Streaming fetch via driver without LOB locator overhead. |
| `BLOB` | `pa.large_binary()` | Streaming fetch for arbitrary binary payloads. |
| `RAW` | `pa.binary()` | Exact byte representation. |

---

## 8. Automated Testing

The unit test suite mocks Oracle Database connections and runs offline without requiring database credentials:

```bash
# Run unit test suite
uv run pytest -v

# Run with coverage report
uv run pytest --cov=oracle_parquet
```

### Test Coverage Highlights
- **Configuration & Secrets**: Validates field bounds, compression codecs, and credential masking in logs/DSNs.
- **SQL Injection Defense**: Rejects malicious identifiers (SQL comments, quotes, union injections, semicolons).
- **Type Translation**: Verifies exact decimal precision, integer sizing, timestamps, and LOB conversion warnings.
- **Parquet Writer**: Tests single-file streaming, multi-file partition rotation, and valid 0-row empty result sets.
- **Validation Suite**: Verifies byte tampering detection, row count discrepancy flagging, and manifest schema verification.
- **Integration Test**: Optional live database test (`tests/test_integration_oracle.py`) which auto-skips when credentials are not configured.

---

## 9. Benchmarks

A reproducible synthetic benchmark harness evaluates serialization, bounded-memory consumption, throughput, and compression ratio without fabricating performance figures:

```bash
uv run python benchmarks/benchmark_export.py
```

### Measured Performance Results (Python 3.12, Linux x86_64, PyArrow 26.0)

| Dataset Rows | Total Time (s) | Throughput (rows/s) | Peak RAM (MB) | Output Size (MB) | ZSTD Compression Ratio | Validation |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10,000** | 0.189s | **54,797** | **3.70 MB** | 0.16 MB | **5.91 : 1** | **PASSED** |
| **100,000** | 1.521s | **66,309** | **22.57 MB** | 1.55 MB | **6.26 : 1** | **PASSED** |
| **1,000,000** | 14.300s | **70,024** | **22.62 MB** | 15.49 MB | **6.33 : 1** | **PASSED** |

> **Memory Bound Guarantee**: Even when exporting 1,000,000 rows, peak RAM utilization remains capped at ~22.6 MB due to the bounded streaming batch generator and direct PyArrow chunk writing.

---

## 10. Security & Observability

- **Read-Only Access**: Configure the pipeline with a read-only database user granted `SELECT` only on target schemas.
- **Strict Identifier Validation**: User inputs are strictly validated against `^[A-Za-z0-9_$#]{1,128}$` and double-quoted. Identifiers are verified against schema metadata before query compilation.
- **Credential Sanitization**: Passwords and wallet tokens are filtered from `safe_dict()`, UI views, and Python log formatters.
- **Job Observability**: Application log records automatically include the active `[job_id]` and write to both `sys.stderr` and `logs/oracle_parquet.log`.

---

## 11. Troubleshooting

| Symptom | Cause | Solution |
| :--- | :--- | :--- |
| `DPI-1047: Cannot locate a 64-bit Oracle Client library` | `ORACLE_CLIENT_MODE=thick` without Instant Client installed | Switch `.env` to `ORACLE_CLIENT_MODE=thin` (pure Python, recommended) or install Oracle Instant Client. |
| `ORA-12541: TNS:no listener` | Database host or port unreachable | Check hostname, port (typically 1521), and network firewall rules. |
| `ORA-01017: invalid username/password` | Incorrect credentials | Verify `ORACLE_USER` and `ORACLE_PASSWORD` in `.env`. |
| `Validation Status: FAILED (Checksum mismatch)` | Parquet file modified or corrupted on disk | Re-run export; verify storage volume health. |

---

## 12. Future Extension Points

Designed with clean interfaces to support:
- Cloud storage uploads (OCI Object Storage, Google Cloud Storage, AWS S3) via dedicated storage plugins.
- Incremental extraction using high-watermark columns or Oracle SCN / Flashback queries.
- Direct ingestion into downstream vector databases (e.g. Qdrant, FAISS) or data lakes (Apache Iceberg, Delta Lake).
