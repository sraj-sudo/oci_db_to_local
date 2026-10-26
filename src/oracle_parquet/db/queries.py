"""Safe SQL query generation with identifier validation and quoting."""

import re

# Oracle valid unquoted identifier pattern (letters, digits, _, $, #)
# Length in Oracle is up to 128 chars (Oracle 12.2+)
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_$#]{1,128}$")


class SafeQueryBuilder:
    """Builds and validates safe Oracle SELECT queries without SQL injection risk."""

    @staticmethod
    def validate_identifier(name: str) -> str:
        """Validate an Oracle schema, table, or column identifier against strict pattern.

        Raises:
            ValueError: If identifier contains illegal characters, whitespace, or invalid length.
        """
        if not isinstance(name, str):
            raise ValueError(f"Identifier must be a string, got {type(name).__name__}")

        stripped = name.strip()
        if not stripped:
            raise ValueError("Identifier cannot be empty")

        if not IDENTIFIER_PATTERN.match(stripped):
            raise ValueError(
                f"Invalid Oracle identifier '{name}'. Identifiers must be 1-128 characters "
                f"and contain only alphanumeric characters, underscores, '$', or '#'."
            )

        return stripped

    @staticmethod
    def quote_identifier(name: str) -> str:
        """Validate and quote an identifier with Oracle double quotes."""
        clean_name = SafeQueryBuilder.validate_identifier(name)
        # Escape any internal double quotes (though regex pattern excludes them)
        escaped = clean_name.replace('"', '""')
        return f'"{escaped}"'

    @classmethod
    def build_select_query(
        cls,
        schema: str,
        table: str,
        columns: list[str],
        limit: int | None = None,
        valid_columns: list[str] | None = None,
        order_by: list[str] | str | None = None,
    ) -> tuple[str, dict]:
        """Construct a validated SELECT query.

        Args:
            schema: Validated Oracle schema name.
            table: Validated Oracle table or view name.
            columns: List of column names to select (in user-requested order).
            limit: Optional maximum row count to fetch.
            valid_columns: Optional whitelist of known columns from metadata.
            order_by: Optional column name or list of columns to order by (e.g. index/PK).

        Returns:
            Tuple of (SQL statement with bind variables, bind parameters dictionary).
        """
        if not columns:
            raise ValueError("At least one column must be selected for extraction")

        # Validate schema and table
        q_schema = cls.quote_identifier(schema)
        q_table = cls.quote_identifier(table)

        # Validate and quote columns
        quoted_columns: list[str] = []
        for col in columns:
            clean_col = cls.validate_identifier(col)
            if valid_columns is not None:
                # Case-insensitive comparison against valid columns from metadata
                valid_lookup = {vc.upper(): vc for vc in valid_columns}
                if clean_col.upper() not in valid_lookup:
                    raise ValueError(f"Column '{clean_col}' is not present in table {schema}.{table}")
            quoted_columns.append(cls.quote_identifier(clean_col))

        columns_clause = ", ".join(quoted_columns)
        from_clause = f"{q_schema}.{q_table}"

        params: dict = {}
        sql = f"SELECT {columns_clause} FROM {from_clause}"

        # Safe ORDER BY clause
        if order_by:
            order_cols = [order_by] if isinstance(order_by, str) else order_by
            quoted_order = [cls.quote_identifier(cls.validate_identifier(oc)) for oc in order_cols]
            sql += f" ORDER BY {', '.join(quoted_order)}"

        if limit is not None and limit > 0:
            sql += " FETCH FIRST :row_limit ROWS ONLY"
            params["row_limit"] = int(limit)

        return sql, params
