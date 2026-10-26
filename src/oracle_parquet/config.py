"""Application configuration using pydantic-settings."""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline configuration settings loaded from environment variables or .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Oracle Connection
    oracle_user: str = Field(default="", description="Oracle database username")
    oracle_password: str = Field(default="", description="Oracle database password")
    oracle_dsn: str = Field(default="", description="Oracle DSN (e.g., host:1521/service_name)")
    oracle_client_mode: Literal["thin", "thick"] = Field(
        default="thin",
        description="Oracle client mode: 'thin' (default pure Python) or 'thick'",
    )
    oracle_client_lib_dir: str | None = Field(
        default=None,
        description="Path to Oracle Instant Client directory for Thick driver mode",
    )
    oracle_wallet_location: str | None = Field(
        default=None,
        description="Path to Oracle Autonomous Database wallet directory",
    )
    oracle_wallet_password: str | None = Field(
        default=None,
        description="Password for Oracle cwallet.sso / ewallet.p12 if encrypted",
    )

    # Django-compatible aliases
    db_host: str | None = Field(default=None, description="Database host")
    db_port: int | None = Field(default=None, description="Database port")
    db_name: str | None = Field(default=None, description="Database service name / SID")
    db_user: str | None = Field(default=None, description="Database username")
    db_password: str | None = Field(default=None, description="Database password")

    # Prototype target defaults
    default_schema: str = Field(default="PRODICS", description="Default selected schema")
    default_table: str = Field(
        default="XXFMICS_MTL_SYSTEM_ITEMS_STG",
        description="Default selected table",
    )

    # Export & Execution Defaults
    export_dir: Path = Field(
        default=Path("data/exports"),
        description="Base directory for export outputs",
    )
    batch_size: int = Field(
        default=50000,
        ge=100,
        le=1_000_000,
        description="Batch size for database cursor fetch and parquet chunk writing",
    )
    parquet_compression: Literal["zstd", "snappy", "gzip", "none"] = Field(
        default="zstd",
        description="Parquet compression codec (zstd recommended default, snappy alternative)",
    )
    preview_row_limit: int = Field(
        default=100,
        ge=1,
        le=10_000,
        description="Maximum row limit for data preview",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Application logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    log_dir: Path = Field(
        default=Path("logs"),
        description="Directory for application log files",
    )

    @field_validator("parquet_compression", mode="before")
    @classmethod
    def normalize_compression(cls, v: str) -> str:
        if isinstance(v, str):
            val = v.strip().lower()
            if val in {"zstd", "snappy", "gzip", "none"}:
                return val
        return "zstd"

    @classmethod
    def _apply_fallbacks(cls, data: dict) -> dict:
        """Fallback to Django-style DB_* environment variables if ORACLE_* is unset."""
        if not data.get("oracle_user") and data.get("db_user"):
            data["oracle_user"] = data["db_user"]
        if not data.get("oracle_password") and data.get("db_password"):
            data["oracle_password"] = data["db_password"]
        if not data.get("oracle_dsn") and data.get("db_host") and data.get("db_name"):
            port = data.get("db_port") or 1521
            data["oracle_dsn"] = f"{data['db_host']}:{port}/{data['db_name']}"
        return data

    @classmethod
    def model_validate(cls, obj, *args, **kwargs):
        if isinstance(obj, dict):
            obj = cls._apply_fallbacks(obj)
        return super().model_validate(obj, *args, **kwargs)

    @property
    def is_configured(self) -> bool:
        """Check if minimum required credentials and DSN are provided."""
        return bool(self.oracle_user.strip() and self.oracle_dsn.strip())

    @property
    def safe_dsn(self) -> str:
        """Return DSN with any inline credentials scrubbed."""
        if not self.oracle_dsn:
            return ""
        # If DSN contains user:pass@ format, mask the password
        if "@" in self.oracle_dsn and ":" in self.oracle_dsn.split("@")[0]:
            prefix, rest = self.oracle_dsn.split("@", 1)
            user = prefix.split(":")[0]
            return f"{user}:***@{rest}"
        return self.oracle_dsn

    @property
    def sqlalchemy_url(self):
        """Construct a SQLAlchemy URL using oracle+oracledb dialect."""
        from sqlalchemy import URL

        host = self.db_host
        port = self.db_port or 1521
        service_name = self.db_name

        if not host and self.oracle_dsn:
            dsn = self.oracle_dsn.strip()
            if "/" in dsn:
                hp, service_name = dsn.split("/", 1)
                if ":" in hp:
                    host, port_str = hp.split(":", 1)
                    port = int(port_str)
                else:
                    host = hp
            else:
                host = dsn

        query: dict[str, str] = {}
        if service_name:
            query["service_name"] = service_name

        return URL.create(
            "oracle+oracledb",
            username=self.oracle_user,
            password=self.oracle_password,
            host=host,
            port=port,
            query=query,
        )

    def safe_dict(self) -> dict:
        """Return dictionary representation with sensitive attributes masked."""
        data = self.model_dump()
        if data.get("oracle_password"):
            data["oracle_password"] = "********"
        if data.get("oracle_wallet_password"):
            data["oracle_wallet_password"] = "********"
        data["oracle_dsn"] = self.safe_dsn
        return data


_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """Return singleton configuration settings instance."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance


def reset_settings() -> None:
    """Reset configuration singleton (useful for testing)."""
    global _settings_instance
    _settings_instance = None
