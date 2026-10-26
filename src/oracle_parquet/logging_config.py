"""Central logging configuration with secret sanitization and job context."""

import logging
import re
import sys
from contextvars import ContextVar
from pathlib import Path

# ContextVar for thread/async-safe tracking of current job_id
current_job_id: ContextVar[str] = ContextVar("current_job_id", default="-")


class JobContextFilter(logging.Filter):
    """Filter that injects the current job_id into LogRecord attributes."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "job_id") or getattr(record, "job_id") == "-":
            record.job_id = current_job_id.get()
        return True


class SecretSanitizingFormatter(logging.Formatter):
    """Formatter that strips sensitive values (passwords, tokens) from log messages."""

    PASSWORD_PATTERNS = [
        re.compile(r"(password\s*[:=]\s*)[^\s,;&'\"]+", re.IGNORECASE),
        re.compile(r"(pwd\s*[:=]\s*)[^\s,;&'\"]+", re.IGNORECASE),
        re.compile(r"(secret\s*[:=]\s*)[^\s,;&'\"]+", re.IGNORECASE),
        re.compile(r"(//[^:]+:)([^@]+)(@)", re.IGNORECASE),
    ]

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        for pattern in self.PASSWORD_PATTERNS:
            formatted = pattern.sub(r"\1***\3" if pattern.groups == 3 else r"\1***", formatted)
        return formatted


def setup_logging(
    log_level: str = "INFO",
    log_dir: Path | str = "logs",
    log_filename: str = "oracle_parquet.log",
) -> logging.Logger:
    """Configure root application logger with console and rotating file outputs."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logger = logging.getLogger("oracle_parquet")
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / log_filename

    fmt = "%(asctime)s | %(levelname)-7s | [%(job_id)s] %(name)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = SecretSanitizingFormatter(fmt=fmt, datefmt=datefmt)
    job_filter = JobContextFilter()

    # Console Handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(job_filter)
    logger.addHandler(console_handler)

    # File Handler
    try:
        from logging.handlers import RotatingFileHandler

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(job_filter)
        logger.addHandler(file_handler)
    except Exception as exc:
        logger.warning("Could not initialize file logger: %s", exc)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a child logger inheriting central formatting and context."""
    return logging.getLogger(f"oracle_parquet.{name}")
