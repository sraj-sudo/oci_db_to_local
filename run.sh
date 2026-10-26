#!/usr/bin/env bash
# Wrapper to set Oracle Instant Client library path and launch the Streamlit application
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export ORACLE_CLIENT_LIB_DIR="${ORACLE_CLIENT_LIB_DIR:-/home/shivesh/code/Boiler_Engineering_Workflow_Automation/Backend/oracle_client/instantclient_21_12}"
export LD_LIBRARY_PATH="${ORACLE_CLIENT_LIB_DIR}:${LD_LIBRARY_PATH}"
PORT="${STREAMLIT_SERVER_PORT:-8504}"

echo "Starting Oracle-to-Parquet Pipeline Streamlit application on port ${PORT}..."
echo "Oracle Client Lib Dir: ${ORACLE_CLIENT_LIB_DIR}"
exec uv run streamlit run "${SCRIPT_DIR}/app.py" --server.port="${PORT}" "$@"
