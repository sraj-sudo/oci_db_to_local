"""Unit tests for configuration validation and credential hygiene."""

from oracle_parquet.config import Settings, get_settings, reset_settings


def test_settings_defaults():
    reset_settings()
    settings = Settings(
        oracle_user="hr_user",
        oracle_password="top_secret_pwd",
        oracle_dsn="dbhost:1521/xepdb1",
    )
    assert settings.oracle_user == "hr_user"
    assert settings.batch_size == 50000
    assert settings.parquet_compression == "zstd"
    assert settings.is_configured is True


def test_credential_masking():
    settings = Settings(
        oracle_user="scott",
        oracle_password="tiger_password",
        oracle_dsn="scott:tiger_password@host:1521/pdb",
        oracle_wallet_password="wallet_secret",
    )
    safe = settings.safe_dict()
    assert safe["oracle_password"] == "********"
    assert safe["oracle_wallet_password"] == "********"
    assert "tiger_password" not in safe["oracle_dsn"]
    assert "scott:***@host:1521/pdb" in safe["oracle_dsn"]


def test_compression_normalization():
    s1 = Settings(parquet_compression="SNAPPY")
    assert s1.parquet_compression == "snappy"

    s2 = Settings(parquet_compression="invalid_codec")
    assert s2.parquet_compression == "zstd"


def test_singleton_get_and_reset():
    reset_settings()
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
    reset_settings()
    s3 = get_settings()
    assert s3 is not s1
