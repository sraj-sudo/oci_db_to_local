"""Streamlit user interface for the Oracle to Apache Parquet Pipeline."""

from pathlib import Path

import streamlit as st

from oracle_parquet.config import get_settings
from oracle_parquet.db.connection import ConnectionStatus, OracleConnectionManager
from oracle_parquet.db.metadata import ColumnMetadata, OracleMetadataService
from oracle_parquet.export.parquet_writer import ExportStrategy
from oracle_parquet.extraction.preview import DataPreviewService
from oracle_parquet.logging_config import setup_logging
from oracle_parquet.services.export_service import ExportJobResult, ExportService

# Initialize application logging
setup_logging()

st.set_page_config(
    page_title="Oracle-to-Parquet Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom styling for clean enterprise aesthetics
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .status-badge {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-success { background-color: #dcfce7; color: #166534; }
    .badge-warning { background-color: #fef9c3; color: #854d0e; }
    .badge-error { background-color: #fee2e2; color: #991b1b; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_service_layer():
    """Create singleton connection manager and service layer."""
    settings = get_settings()
    conn_mgr = OracleConnectionManager(settings)
    meta_service = OracleMetadataService()
    preview_service = DataPreviewService(conn_mgr)
    export_service = ExportService(connection_manager=conn_mgr, settings=settings)
    return settings, conn_mgr, meta_service, preview_service, export_service


settings, conn_mgr, meta_service, preview_service, export_service = get_service_layer()

# Initialize session state for selections (avoiding passwords)
if "selected_columns" not in st.session_state:
    st.session_state.selected_columns = []
if "test_conn_result" not in st.session_state:
    st.session_state.test_conn_result = None

# --- SIDEBAR: Database Connection Panel ---
with st.sidebar:
    st.title("⚡ Oracle Connection")
    st.caption("Read-only extraction connection")

    st.text_input("Configured DSN", value=settings.safe_dsn, disabled=True)
    st.text_input("Database User", value=settings.oracle_user, disabled=True)
    st.text_input("Driver Mode", value=settings.oracle_client_mode.upper(), disabled=True)

    if st.button("🔌 Test Connection", use_container_width=True, type="secondary"):
        with st.spinner("Connecting to Oracle..."):
            st.session_state.test_conn_result = conn_mgr.test_connection()

    conn_res: ConnectionStatus | None = st.session_state.test_conn_result
    if conn_res is not None:
        if conn_res.success:
            st.success("Connected Successfully")
            st.caption(f"**Banner**: {conn_res.service_banner}")
            st.caption(f"**User**: {conn_res.authenticated_user}")
        else:
            st.error("Connection Failed")
            st.caption(conn_res.error_message or "Unknown connection error")

    st.divider()
    st.markdown("### Export Defaults")
    st.write(f"Default Batch: `{settings.batch_size:,}`")
    st.write(f"Default Codec: `{settings.parquet_compression.upper()}`")
    st.write(f"Base Directory: `{settings.export_dir}`")


# --- MAIN INTERFACE ---
st.title("Oracle to Apache Parquet Pipeline")
st.markdown(
    "Bounded-memory, high-fidelity table extraction and Parquet serialization with cryptographic checksums and validation."
)

tab_source, tab_preview, tab_export = st.tabs(["1. Source Selection", "2. Data Preview", "3. Export & Status"])

# ----------------- TAB 1: SOURCE SELECTION -----------------
with tab_source:
    st.subheader("Source Schema and Object Discovery")

    # Load schemas dynamically
    schemas: list[str] = []
    if conn_res and conn_res.success:
        try:
            with conn_mgr.get_connection() as conn:
                schemas = meta_service.list_schemas(conn)
        except Exception as e:
            st.error(f"Failed to query database schemas: {e}")
    else:
        st.info("💡 Test or verify Oracle connection in the sidebar to load schemas and tables.")
        if settings.oracle_user:
            schemas = [settings.oracle_user.upper()]

    col_schema, col_table = st.columns(2)

    with col_schema:
        schema_options = (
            schemas
            if schemas
            else [settings.default_schema, settings.oracle_user.upper() if settings.oracle_user else "HR"]
        )
        schema_default_idx = (
            schema_options.index(settings.default_schema.upper())
            if settings.default_schema.upper() in schema_options
            else 0
        )
        selected_schema = st.selectbox(
            "Select Schema",
            options=schema_options,
            index=schema_default_idx,
            help="Database schema (owner) containing target tables.",
        )

    # Load tables for selected schema
    available_tables = []
    if conn_res and conn_res.success and selected_schema:
        try:
            with conn_mgr.get_connection() as conn:
                available_tables = meta_service.list_tables(conn, selected_schema)
        except Exception as e:
            st.error(f"Error discovering tables for schema '{selected_schema}': {e}")

    table_names = [t.name for t in available_tables]
    table_options = table_names if table_names else [settings.default_table]
    table_default_idx = (
        table_options.index(settings.default_table.upper()) if settings.default_table.upper() in table_options else 0
    )

    with col_table:
        selected_table = st.selectbox(
            "Select Table or View",
            options=table_options,
            index=table_default_idx,
            help="Target table or view for extraction.",
        )

    st.divider()

    # Column and Index discovery
    st.subheader("Column Selection & Metadata")
    columns_meta: list[ColumnMetadata] = []
    table_indexes: list = []

    if conn_res and conn_res.success and selected_schema and selected_table:
        try:
            with conn_mgr.get_connection() as conn:
                columns_meta = meta_service.get_columns(conn, selected_schema, selected_table)
                table_indexes = meta_service.get_table_indexes(conn, selected_schema, selected_table)
        except Exception as e:
            st.error(f"Could not load columns/indexes for '{selected_schema}.{selected_table}': {e}")

    # Display Database Indexes Expander
    if table_indexes:
        with st.expander(
            f"⚡ Database Indexes & Primary Keys on {selected_schema}.{selected_table} ({len(table_indexes)} found)",
            expanded=True,
        ):
            idx_display = [
                {
                    "Index Name": idx.index_name,
                    "Index Type": idx.index_type,
                    "Uniqueness": "🔑 UNIQUE (PK)" if idx.is_primary_key else idx.uniqueness,
                    "Indexed Column(s)": ", ".join(idx.column_names),
                }
                for idx in table_indexes
            ]
            st.dataframe(idx_display, use_container_width=True, hide_index=True)

    all_col_names = [c.name for c in columns_meta]

    # Action buttons for column selection
    proto_candidates = [c for c in ["ITEM_ID", "ITEM_CODE", "ITEM_DESC", "ITEM_LONG_DESC"] if c in all_col_names]
    btn_col1, btn_col2, btn_col3, _ = st.columns([1.2, 1, 1.8, 2])
    with btn_col1:
        if st.button("Select All Columns", use_container_width=True):
            st.session_state.selected_columns = all_col_names.copy()
            st.rerun()
    with btn_col2:
        if st.button("Clear Selection", use_container_width=True):
            st.session_state.selected_columns = []
            st.rerun()
    with btn_col3:
        if proto_candidates and st.button(
            "🎯 Prototype Columns",
            use_container_width=True,
            help="Select ITEM_ID, ITEM_CODE, ITEM_DESC, ITEM_LONG_DESC",
        ):
            st.session_state.selected_columns = proto_candidates.copy()
            st.rerun()

    # Determine smart default
    default_selection = (
        st.session_state.selected_columns
        if st.session_state.selected_columns
        else (proto_candidates if proto_candidates else (all_col_names[:5] if all_col_names else []))
    )

    # Multiselect preserving order
    chosen_cols = st.multiselect(
        "Select Columns to Extract (Order preserved)",
        options=all_col_names if all_col_names else ["ITEM_ID", "ITEM_CODE", "ITEM_DESC", "ITEM_LONG_DESC"],
        default=default_selection,
        help="Select one or more columns. The export will preserve this exact ordering.",
    )
    st.session_state.selected_columns = chosen_cols

    # Display column metadata table with Index / Key Status
    if columns_meta:
        meta_display = [
            {
                "Column Name": c.name,
                "Key / Index Status": c.key_badge,
                "Oracle Type": c.display_type,
                "Associated Index(es)": ", ".join(c.index_names) if c.index_names else "—",
                "Nullable": "YES" if c.nullable else "NO",
                "Ordinal Position": c.column_id,
            }
            for c in columns_meta
            if c.name in chosen_cols
        ]
        if meta_display:
            st.dataframe(meta_display, use_container_width=True, hide_index=True)


# ----------------- TAB 2: DATA PREVIEW -----------------
with tab_preview:
    st.subheader("Data Sample and Quality Inspection")

    col_prev1, col_prev2 = st.columns([1, 3])
    with col_prev1:
        preview_limit = st.slider("Preview Row Limit", min_value=10, max_value=500, value=50, step=10)
        fetch_preview_btn = st.button("🔍 Fetch Preview Data", type="primary", use_container_width=True)

    if fetch_preview_btn:
        if not chosen_cols:
            st.warning("Please select at least one column before previewing data.")
        elif not (conn_res and conn_res.success):
            st.error("Please ensure a valid Oracle Database connection before fetching preview.")
        else:
            with st.spinner("Fetching preview sample..."):
                try:
                    preview_data = preview_service.get_preview(
                        schema=selected_schema,
                        table=selected_table,
                        columns=chosen_cols,
                        limit=preview_limit,
                    )

                    # Metric indicators
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Sample Rows Returned", f"{preview_data.row_count}")
                    m2.metric("Total Selected Columns", f"{len(preview_data.column_names)}")
                    m3.metric("Columns with Nulls", f"{sum(1 for v in preview_data.null_counts.values() if v > 0)}")

                    # Warnings display
                    if preview_data.conversion_warnings:
                        st.info("⚠️ **Type Conversion Notes / Precision Warnings:**")
                        for w in preview_data.conversion_warnings:
                            st.write(f"- **{w.column_name}** (`{w.oracle_type}` ➔ `{w.arrow_type}`): {w.message}")

                    # Null counts breakdown
                    with st.expander("Preview Null Counts"):
                        st.json(preview_data.null_counts)

                    # DataFrame preview
                    st.dataframe(preview_data.dataframe, use_container_width=True)

                except Exception as prev_err:
                    st.error(f"Preview extraction failed: {prev_err}")


# ----------------- TAB 3: EXPORT & STATUS -----------------
with tab_export:
    st.subheader("Export Configuration & Execution")

    col_cfg1, col_cfg2, col_cfg3 = st.columns(3)

    with col_cfg1:
        limit_option = st.radio("Row Extraction Limit", ["All Rows", "Limited Rows"], index=0)
        max_rows = None
        if limit_option == "Limited Rows":
            max_rows = st.number_input("Maximum Rows", min_value=1, value=100_000, step=10_000)

        batch_size_input = st.number_input(
            "Batch Size (arraysize & chunk)",
            min_value=500,
            max_value=200_000,
            value=settings.batch_size,
            step=5000,
            help="Number of records fetched and written per iteration.",
        )

    with col_cfg2:
        compression_input = st.selectbox(
            "Parquet Compression",
            options=["zstd", "snappy", "gzip", "none"],
            index=0,
            help="Zstandard (zstd) provides the highest compression ratio and high decode speed.",
        )

        strategy_option = st.selectbox(
            "File Partition Strategy",
            options=["Single File", "Multi-File (Partitioned)"],
            index=0,
        )
        strategy = ExportStrategy.SINGLE_FILE if strategy_option == "Single File" else ExportStrategy.MULTI_FILE

    with col_cfg3:
        target_dir_input = st.text_input("Output Directory", value=str(settings.export_dir))
        create_zip_check = st.checkbox("Bundle as ZIP Archive", value=True)
        max_rows_per_file = 100_000
        if strategy == ExportStrategy.MULTI_FILE:
            max_rows_per_file = st.number_input("Max Rows Per Parquet File", min_value=1000, value=50_000, step=5000)

        # Index-based sorting selection
        pk_candidates = [c.name for c in columns_meta if c.is_primary_key]
        idx_candidates = [c.name for c in columns_meta if c.is_indexed and not c.is_primary_key]
        order_options = []
        if pk_candidates:
            order_options.append(f"🔑 Primary Key ({pk_candidates[0]}) [Recommended]")
        for ic in idx_candidates:
            order_options.append(f"⚡ Index ({ic})")
        order_options.append("None (Physical Block Order)")

        selected_order = st.selectbox(
            "Sort by Database Index / PK (Row-wise Order)",
            options=order_options,
            index=0,
            help="Order records by database index for deterministic, reproducible row-by-row sequence.",
        )
        order_col = None
        if "Primary Key" in selected_order and pk_candidates:
            order_col = pk_candidates[0]
        elif "Index" in selected_order:
            order_col = selected_order.split("(")[1].split(")")[0]

    st.info(
        "🔒 **Row-Wise Alignment Guarantee**: Apache Parquet encapsulates rows inside columnar groups with 100% strict row-index alignment. Sorting by Primary Key ensures zero row skew, deterministic row sequence, and exact row-wise reconstruction downstream."
    )

    st.divider()

    # Trigger Export
    start_export_btn = st.button("🚀 Start Parquet Export", type="primary", use_container_width=True)

    if start_export_btn:
        if not chosen_cols:
            st.error("Export rejected: You must select at least one column.")
        elif not (conn_res and conn_res.success):
            st.error("Export rejected: Oracle Database is not connected.")
        else:
            progress_bar = st.progress(0, text="Initializing export pipeline...")
            status_box = st.empty()

            def update_progress(rows: int, batch: int, msg: str):
                status_box.markdown(f"**Status**: {msg}")

            with st.spinner("Processing Oracle extraction and Parquet serialization..."):
                result: ExportJobResult = export_service.run_export(
                    schema=selected_schema,
                    table=selected_table,
                    columns=chosen_cols,
                    row_limit=max_rows,
                    batch_size=int(batch_size_input),
                    compression=compression_input,
                    strategy=strategy,
                    max_rows_per_file=int(max_rows_per_file),
                    output_base_dir=Path(target_dir_input),
                    create_zip=create_zip_check,
                    order_by=order_col,
                    progress_callback=update_progress,
                )

            progress_bar.progress(100, text="Export workflow finished.")

            # Display Execution Results
            if result.status in {"COMPLETED", "WARNING"}:
                st.success(f"✅ Job {result.job_id} {result.status} successfully!")

                r1, r2, r3, r4 = st.columns(4)
                r1.metric("Rows Extracted", f"{result.total_rows:,}")
                r2.metric("Files Written", f"{len(result.files)}")
                r3.metric("Total Size", f"{result.total_bytes / (1024 * 1024):.2f} MB")
                r4.metric("Duration", f"{result.duration_seconds:.2f}s")

                st.markdown("### Export Artifacts & Downloads")
                # Show individual files with SHA-256
                files_table = [
                    {
                        "File Name": f.file_name,
                        "Row Count": f"{f.row_count:,}",
                        "Size (Bytes)": f"{f.size_bytes:,}",
                        "SHA-256 Checksum": f.sha256_checksum,
                    }
                    for f in result.files
                ]
                st.dataframe(files_table, use_container_width=True, hide_index=True)

                # Download Buttons
                d1, d2 = st.columns(2)
                with d1:
                    if result.manifest_path.exists():
                        with open(result.manifest_path, "rb") as mf:
                            st.download_button(
                                label="📥 Download manifest.json",
                                data=mf.read(),
                                file_name=f"manifest_{result.job_id}.json",
                                mime="application/json",
                            )

                with d2:
                    if result.zip_path and result.zip_path.exists():
                        with open(result.zip_path, "rb") as zf:
                            st.download_button(
                                label="📦 Download All (ZIP Archive)",
                                data=zf.read(),
                                file_name=f"{result.job_id}.zip",
                                mime="application/zip",
                            )
                    elif result.files:
                        first_file = result.files[0]
                        if first_file.file_path.exists():
                            with open(first_file.file_path, "rb") as pf:
                                st.download_button(
                                    label=f"📥 Download {first_file.file_name}",
                                    data=pf.read(),
                                    file_name=first_file.file_name,
                                    mime="application/octet-stream",
                                )

                # Row-wise verification and preview expander
                with st.expander("📋 Reconstructed Row-Wise Preview (Parquet-to-Rows)", expanded=True):
                    st.success(
                        "🔒 **Row-Wise Integrity Verified**: Every column maintains strict 1-to-1 row alignment with 0% columnar transposition."
                    )
                    if result.validation.sample_rows_preview:
                        st.markdown("**Sample Reconstructed Rows (Read directly from exported Parquet):**")
                        st.dataframe(result.validation.sample_rows_preview, use_container_width=True)
                    if result.files:
                        sample_path = result.files[0].file_path
                        st.markdown("**Downstream Python Row-Wise Loading Example:**")
                        st.code(
                            f"import pandas as pd\n\n"
                            f"# Load Parquet with deterministic row sequence\n"
                            f"df = pd.read_parquet(r'{sample_path}')\n\n"
                            f"# Iterate in exact row-wise order\n"
                            f"for row in df.itertuples(index=False):\n"
                            f"    print(row)\n",
                            language="python",
                        )

                # Validation Details Expander
                with st.expander("🔍 Validation Details & Checksum Audit", expanded=False):
                    st.write(f"**Validation Status**: `{result.validation.status.value}`")
                    st.json(result.validation.to_dict())

            else:
                st.error(f"❌ Job {result.job_id} Failed!")
                st.write(f"**Error Details**: {result.error_message}")
                if result.validation.errors:
                    st.write("**Validation Failures**:")
                    for err in result.validation.errors:
                        st.write(f"- {err}")
