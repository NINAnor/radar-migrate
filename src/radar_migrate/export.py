import logging
import os
from dataclasses import dataclass

import duckdb
from jinja2 import Template
from osgeo import gdal

gdal.UseExceptions()

POSTGIS_RASTER_CONNECTION_TEMPLATE = Template(
    "PG:host={{host}} port={{port}} dbname='{{database}}' user='{{username}}'"
    "{% if password %} password='{{password}}'{% endif %}"
    " schema='{{schema}}' table='{{table}}' column='{{raster_column}}' mode=2"
)


@dataclass
class PostgresConnectionInfo:
    """PostgreSQL connection information for GDAL."""

    host: str
    port: int
    database: str
    username: str
    password: str | None = None
    schema: str = "public"

    def to_gdal_connection_string(self, table: str, raster_column: str = "rast") -> str:
        """Build a GDAL PostGIS Raster connection string."""
        return POSTGIS_RASTER_CONNECTION_TEMPLATE.render(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            schema=self.schema,
            table=table,
            raster_column=raster_column,
        )


@dataclass
class S3ConnectionInfo:
    """S3 connection information for GDAL."""

    endpoint: str
    access_key: str
    secret_key: str
    url_style: str = "path"

    def configure_gdal(self) -> None:
        """Configure GDAL with S3 credentials."""
        gdal.SetConfigOption("AWS_S3_ENDPOINT", self.endpoint)
        gdal.SetConfigOption("AWS_ACCESS_KEY_ID", self.access_key)
        gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", self.secret_key)
        gdal.SetConfigOption("AWS_HTTPS", "YES")
        gdal.SetConfigOption("AWS_VIRTUAL_HOSTING", "FALSE")
        if self.url_style == "path":
            gdal.SetConfigOption("AWS_VIRTUAL_HOSTING", "FALSE")
        else:
            gdal.SetConfigOption("AWS_VIRTUAL_HOSTING", "TRUE")

        os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"


def _s3_to_vsis3_path(s3_path: str) -> str:
    """Convert s3://bucket/path to /vsis3/bucket/path."""
    if s3_path.startswith("s3://"):
        return "/vsis3/" + s3_path[5:]
    return s3_path


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


def export_raster_table(
    pg_conn_info: PostgresConnectionInfo,
    table_name: str,
    output_path: str,
    s3_conn_info: S3ConnectionInfo | None = None,
    logger: logging.Logger = None,
    raster_column: str = "rast",
    compression: str = "LZW",
) -> None:
    """
    Export a PostGIS raster table to a Cloud Optimized GeoTIFF.

    Uses GDAL to read from PostGIS raster and write a COG without reprojecting.
    Overviews are built internally for efficient access.

    Args:
        pg_conn_info: PostgreSQL connection information.
        table_name: The name of the raster table to export.
        output_path: The path where the COG will be saved (supports s3:// paths).
        s3_conn_info: S3 connection info (required if output_path is an S3 path).
        raster_column: The name of the raster column (default: rast).
        compression: Compression algorithm (default: LZW).
    """
    # Configure S3 if output is an S3 path
    if output_path.startswith("s3://"):
        if s3_conn_info is None:
            raise ValueError("s3_conn_info is required for S3 output paths")
        s3_conn_info.configure_gdal()
        gdal_output_path = _s3_to_vsis3_path(output_path)
    else:
        gdal_output_path = output_path

    connection_string = pg_conn_info.to_gdal_connection_string(
        table_name, raster_column
    )

    logger.debug(
        "Opening PostGIS raster with GDAL",
        connection_string=connection_string,
    )

    src_ds = gdal.Open(connection_string, gdal.GA_ReadOnly)
    if src_ds is None:
        raise RuntimeError(f"Failed to open PostGIS raster table: {table_name}")

    # COG creation options
    cog_options = [
        "COMPRESS=" + compression,
        "BIGTIFF=IF_SAFER",
        "OVERVIEWS=IGNORE_EXISTING",
    ]

    # Use gdal.Translate to create the COG directly
    # This avoids intermediate files and handles everything in one pass
    translate_options = gdal.TranslateOptions(
        format="COG",
        creationOptions=cog_options,
    )

    out_ds = gdal.Translate(gdal_output_path, src_ds, options=translate_options)

    if out_ds is None:
        raise RuntimeError(f"Failed to create COG at: {output_path}")

    # Close datasets
    out_ds = None
    src_ds = None


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
