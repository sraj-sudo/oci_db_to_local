"""Oracle Database Connection Management supporting SQLAlchemy Engine and Connections."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

import oracledb
from sqlalchemy import Connection, Engine, create_engine, text

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
    """Manages creation, verification, and connection pooling via SQLAlchemy."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._engine: Engine | None = None
        self._ensure_client_mode()

    def _ensure_client_mode(self) -> None:
        """Initialize thick mode if configured and not already initialized."""
        global _THICK_MODE_INITIALIZED
        if self.settings.oracle_client_mode == "thick" and not _THICK_MODE_INITIALIZED:
            try:
                lib_dir = self.settings.oracle_client_lib_dir
                if lib_dir:
                    oracledb.init_oracle_client(lib_dir=str(lib_dir))
                    logger.info("Initialized Oracle Database Thick Client mode with lib_dir=%s", lib_dir)
                else:
                    oracledb.init_oracle_client()
                    logger.info("Initialized Oracle Database Thick Client mode")
                _THICK_MODE_INITIALIZED = True
            except Exception as exc:
                logger.error("Failed to initialize Oracle Thick mode: %s", exc)
                raise RuntimeError(
                    f"Oracle Thick Client initialization failed. Ensure Oracle Client libraries "
                    f"are installed or switch to 'thin' mode: {exc}"
                ) from exc

    def get_engine(self) -> Engine:
        """Create or return the cached SQLAlchemy Engine instance."""
        if self._engine is None:
            thick_kwargs: dict = {}
            if self.settings.oracle_client_mode == "thick":
                lib_dir = self.settings.oracle_client_lib_dir
                thick_kwargs["thick_mode"] = {"lib_dir": str(lib_dir)} if lib_dir else True

            connect_args: dict = {
                "retry_count": 5,
                "retry_delay": 1,
                "tcp_connect_timeout": 5.0,
                "expire_time": 2,
            }
            if self.settings.oracle_wallet_location:
                connect_args["wallet_location"] = str(self.settings.oracle_wallet_location)
            if self.settings.oracle_wallet_password:
                connect_args["wallet_password"] = str(self.settings.oracle_wallet_password)

            self._engine = create_engine(
                self.settings.sqlalchemy_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args=connect_args,
                **thick_kwargs,
            )
            logger.info("Created SQLAlchemy Engine for Oracle DSN: %s", self.settings.safe_dsn)
        return self._engine

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """Context manager providing a managed SQLAlchemy Connection."""
        engine = self.get_engine()
        try:
            with engine.connect() as conn:
                yield conn
        except oracledb.Error as err:
            clean_msg = err.args[0].message if err.args and hasattr(err.args[0], "message") else str(err)
            logger.error("Oracle database error: %s", clean_msg)
            raise ConnectionError(f"Oracle connection error: {clean_msg}") from err
        except Exception as exc:
            logger.error("Unexpected database error: %s", exc)
            raise ConnectionError(f"Database error: {exc}") from exc

    def test_connection(self) -> ConnectionStatus:
        """Test database connection and retrieve banner / user verification via SQLAlchemy."""
        if not self.settings.is_configured:
            return ConnectionStatus(
                success=False,
                error_message="Database credentials or DSN not configured. Please check .env settings.",
                client_mode=self.settings.oracle_client_mode,
            )

        try:
            with self.get_connection() as conn:
                banner_res = conn.execute(text("SELECT banner FROM v$version WHERE ROWNUM = 1")).scalar()
                banner = banner_res or "Oracle Database"

                user_res = conn.execute(text("SELECT USER FROM DUAL")).scalar()
                current_user = user_res or self.settings.oracle_user

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
