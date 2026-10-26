"""Oracle Database Connection Management supporting Thin and Thick modes."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

import oracledb

from oracle_parquet.config import Settings, get_settings
from oracle_parquet.logging_config import get_logger

logger = get_logger("db.connection")

# Global flag to track if thick mode client has been initialized
_THICK_MODE_INITIALIZED = False


@dataclass(frozen=True)
class ConnectionStatus:
    """Represents the outcome of a connection test."""

    success: bool
    service_banner: str = ""
    authenticated_user: str = ""
    error_message: str | None = None
    client_mode: str = "thin"


class OracleConnectionManager:
    """Manages creation, verification, and teardown of Oracle Database connections."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._ensure_client_mode()

    def _ensure_client_mode(self) -> None:
        """Initialize thick mode if configured and not already initialized."""
        global _THICK_MODE_INITIALIZED
        if self.settings.oracle_client_mode == "thick" and not _THICK_MODE_INITIALIZED:
            try:
                # Initialize thick mode with default Oracle Client location or system libraries
                oracledb.init_oracle_client()
                _THICK_MODE_INITIALIZED = True
                logger.info("Initialized Oracle Database Thick Client mode")
            except Exception as exc:
                logger.error("Failed to initialize Oracle Thick mode: %s", exc)
                raise RuntimeError(
                    f"Oracle Thick Client initialization failed. Ensure Oracle Client libraries "
                    f"are installed or switch to 'thin' mode: {exc}"
                ) from exc

    def _build_connection_params(self) -> dict:
        """Construct connection parameters dict safely without leaking credentials in logs."""
        params: dict = {
            "user": self.settings.oracle_user,
            "password": self.settings.oracle_password,
            "dsn": self.settings.oracle_dsn,
        }
        if self.settings.oracle_wallet_location:
            params["wallet_location"] = self.settings.oracle_wallet_location
        if self.settings.oracle_wallet_password:
            params["wallet_password"] = self.settings.oracle_wallet_password

        return params

    @contextmanager
    def get_connection(self) -> Generator[oracledb.Connection, None, None]:
        """Context manager providing a managed Oracle database connection."""
        conn = None
        try:
            params = self._build_connection_params()
            logger.debug(
                "Connecting to Oracle DSN: %s as user: %s (mode: %s)",
                self.settings.safe_dsn,
                self.settings.oracle_user,
                self.settings.oracle_client_mode,
            )
            conn = oracledb.connect(**params)
            yield conn
        except oracledb.Error as err:
            (err_obj,) = err.args
            clean_msg = err_obj.message if hasattr(err_obj, "message") else str(err)
            logger.error("Oracle database error during connection: %s", clean_msg)
            raise ConnectionError(f"Oracle connection error: {clean_msg}") from err
        except Exception as exc:
            logger.error("Unexpected error connecting to Oracle: %s", exc)
            raise ConnectionError(f"Database connection failed: {exc}") from exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                    logger.debug("Oracle connection closed successfully")
                except Exception as close_err:
                    logger.warning("Error closing Oracle connection: %s", close_err)

    def test_connection(self) -> ConnectionStatus:
        """Test database connection and retrieve banner / user verification."""
        if not self.settings.is_configured:
            return ConnectionStatus(
                success=False,
                error_message="Database credentials or DSN not configured. Please check .env settings.",
                client_mode=self.settings.oracle_client_mode,
            )

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Query system banner and current user
                    cursor.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
                    row = cursor.fetchone()
                    banner = row[0] if row else "Oracle Database"

                    cursor.execute("SELECT USER FROM DUAL")
                    user_row = cursor.fetchone()
                    current_user = user_row[0] if user_row else self.settings.oracle_user

                    return ConnectionStatus(
                        success=True,
                        service_banner=banner,
                        authenticated_user=current_user,
                        client_mode=self.settings.oracle_client_mode,
                    )
        except ConnectionError as ce:
            return ConnectionStatus(
                success=False,
                error_message=str(ce),
                client_mode=self.settings.oracle_client_mode,
            )
        except Exception as exc:
            return ConnectionStatus(
                success=False,
                error_message=f"Connection test failed: {exc}",
                client_mode=self.settings.oracle_client_mode,
            )
