import duckdb


def export_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str,
    overwrite: bool = True,
) -> None:
    """
    Export a DuckDB table to a Parquet file.

    Args:
        conn: A DuckDB connection object.
        table_name: The name of the table to export.
        output_path: The path where the Parquet file will be saved.
    """
    conn.table(table_name).write_parquet(
        output_path, compression="zstd", overwrite=overwrite
    )


def export_date_partitioned_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_path: str,
    date_column: str,
    overwrite: bool = True,
) -> None:
    """
    Export a DuckDB table to Parquet files partitioned by a date column.

    Args:
        conn: A DuckDB connection object.
        table_name: The name of the table to export.
        output_path: The path where the Parquet files will be saved.
        date_column: The name of the date column to partition by.
        overwrite: Whether to overwrite existing files.
    """

    DATE_PARTS = ["year", "month", "day"]
    partition_columns = [f"{date_column}_{date_part}" for date_part in DATE_PARTS]

    partitions = [
        duckdb.SQLExpression(f"date_part('{date_part}', {date_column})").alias(
            f"{date_column}_{date_part}"
        )
        for date_part in DATE_PARTS
    ]

    t = conn.table(table_name).select(duckdb.StarExpression(), *partitions)

    t.write_parquet(
        output_path,
        partition_by=partition_columns,
        compression="zstd",
        overwrite=overwrite,
    )
