"""Oracle to Apache Arrow type mapping with precision preservation and conversion reporting."""

import decimal
from dataclasses import dataclass

import pyarrow as pa

from oracle_parquet.db.metadata import ColumnMetadata
from oracle_parquet.logging_config import get_logger

logger = get_logger("extraction.types")


@dataclass(frozen=True)
class ConversionWarning:
    """Records a type translation nuance, potential precision constraint, or fallback."""

    column_name: str
    oracle_type: str
    arrow_type: str
    message: str


class OracleTypeMapper:
    """Translates Oracle column metadata into PyArrow types and validates data fidelity."""

    @classmethod
    def map_column(cls, col: ColumnMetadata) -> tuple[pa.Field, ConversionWarning | None]:
        """Convert a single ColumnMetadata into a PyArrow Field and an optional warning.

        Preserves numeric scale/precision, temporal precision, and character types.
        """
        raw_type = col.data_type.upper().strip()
        name = col.name
        nullable = col.nullable
        warning: ConversionWarning | None = None

        # 1. String and Character Types (preserve identifiers with leading zeros)
        if raw_type in {"VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "VARCHAR"}:
            arrow_type = pa.string()

        # 2. LOB and Long Text / Binary
        elif raw_type in {"CLOB", "NCLOB", "LONG"}:
            arrow_type = pa.large_string()
            warning = ConversionWarning(
                column_name=name,
                oracle_type=col.display_type,
                arrow_type=str(arrow_type),
                message="Large Character Object (CLOB) mapped to Arrow large_string.",
            )
        elif raw_type in {"BLOB", "LONG RAW"}:
            arrow_type = pa.large_binary()
            warning = ConversionWarning(
                column_name=name,
                oracle_type=col.display_type,
                arrow_type=str(arrow_type),
                message="Large Binary Object (BLOB) mapped to Arrow large_binary.",
            )
        elif raw_type in {"RAW"}:
            arrow_type = pa.binary()

        # 3. Numeric Types (preserve exact precision and scale)
        elif raw_type == "NUMBER":
            p = col.data_precision
            s = col.data_scale

            if p is not None and s is not None:
                if s == 0:
                    # Integer representation based on precision
                    if p <= 4:
                        arrow_type = pa.int16()
                    elif p <= 9:
                        arrow_type = pa.int32()
                    elif p <= 18:
                        arrow_type = pa.int64()
                    elif p <= 38:
                        arrow_type = pa.decimal128(p, 0)
                    else:
                        arrow_type = pa.string()
                        warning = ConversionWarning(
                            column_name=name,
                            oracle_type=col.display_type,
                            arrow_type="string",
                            message=f"NUMBER precision {p} exceeds Arrow decimal128 limit; mapped to string.",
                        )
                else:
                    # Fixed decimal with scale
                    if p <= 38 and 0 <= s <= 38 and s <= p:
                        arrow_type = pa.decimal128(p, s)
                    elif p <= 38 and s < 0:
                        # Negative scale in Oracle rounds to powers of 10
                        arrow_type = pa.decimal128(min(38, p + abs(s)), 0)
                        warning = ConversionWarning(
                            column_name=name,
                            oracle_type=col.display_type,
                            arrow_type=str(arrow_type),
                            message=f"Negative scale ({s}) mapped to adjusted decimal.",
                        )
                    else:
                        arrow_type = pa.float64()
                        warning = ConversionWarning(
                            column_name=name,
                            oracle_type=col.display_type,
                            arrow_type="float64",
                            message=f"NUMBER({p},{s}) exceeds standard decimal limits; mapped to float64.",
                        )
            elif p is not None and s is None:
                # Precision defined without scale in Oracle defaults to scale 0
                if p <= 4:
                    arrow_type = pa.int16()
                elif p <= 9:
                    arrow_type = pa.int32()
                elif p <= 18:
                    arrow_type = pa.int64()
                else:
                    arrow_type = pa.decimal128(min(p, 38), 0)
            else:
                # Unconstrained NUMBER (FLOAT in Oracle semantics, up to 38 digits)
                # Map to float64 with explicit conversion warning policy
                arrow_type = pa.float64()
                warning = ConversionWarning(
                    column_name=name,
                    oracle_type=col.display_type,
                    arrow_type="float64",
                    message="Unbounded Oracle NUMBER mapped to float64; potential precision loss for >15 digits.",
                )

        elif raw_type == "FLOAT":
            arrow_type = pa.float64()
        elif raw_type == "BINARY_FLOAT":
            arrow_type = pa.float32()
        elif raw_type == "BINARY_DOUBLE":
            arrow_type = pa.float64()

        # 4. Temporal Types
        elif raw_type == "DATE":
            # Oracle DATE includes year, month, day, hour, min, second -> microsecond timestamp
            arrow_type = pa.timestamp("us")
        elif "TIMESTAMP" in raw_type:
            if "TIME ZONE" in raw_type:
                # Timestamp with timezone -> UTC normalized timestamp
                arrow_type = pa.timestamp("us", tz="UTC")
            else:
                arrow_type = pa.timestamp("us")

        # 5. Rowid & Special Oracle Identifiers
        elif raw_type in {"ROWID", "UROWID"}:
            arrow_type = pa.string()

        # 6. Fallback / Unknown types
        else:
            arrow_type = pa.string()
            warning = ConversionWarning(
                column_name=name,
                oracle_type=col.display_type,
                arrow_type="string",
                message=f"Oracle type '{raw_type}' has no direct Arrow counterpart; converted to string.",
            )

        field = pa.field(name, arrow_type, nullable=nullable)
        return field, warning

    @classmethod
    def build_arrow_schema(cls, columns: list[ColumnMetadata]) -> tuple[pa.Schema, list[ConversionWarning]]:
        """Construct a complete PyArrow Schema preserving original column order."""
        fields: list[pa.Field] = []
        warnings: list[ConversionWarning] = []

        for col in columns:
            field, warn = cls.map_column(col)
            fields.append(field)
            if warn:
                warnings.append(warn)

        schema = pa.schema(fields)
        return schema, warnings

    @classmethod
    def sanitize_value_for_arrow(cls, val: any, arrow_type: pa.DataType) -> any:
        """Sanitize and coerce single cell values if driver returns types requiring conversion."""
        if val is None:
            return None

        # If target is decimal128 and driver returned float or str or int
        if pa.types.is_decimal(arrow_type):
            if isinstance(val, (int, str)):
                return decimal.Decimal(str(val))
            if isinstance(val, float):
                return decimal.Decimal(str(val))
            if isinstance(val, decimal.Decimal):
                return val

        # If driver returned LOB locator (when fetch_lobs was True)
        if hasattr(val, "read"):
            try:
                return val.read()
            except Exception:
                return str(val)

        return val
