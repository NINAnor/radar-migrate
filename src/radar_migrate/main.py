#!/usr/bin/env python3

"""Main script."""

import logging
import pathlib

import duckdb
import environ
import hydra
import structlog
from jinja2 import Template
from omegaconf import DictConfig

from radar_migrate.export import (
    PostgresConnectionInfo,
    S3ConnectionInfo,
    export_date_partitioned_table,
    export_raster_table,
    export_table,
    get_geometry_crs,
)

env = environ.Env()
BASE_DIR = pathlib.Path(__file__).parent
environ.Env.read_env(str(BASE_DIR / ".env"))


def configure_logger(logging_level=logging.NOTSET):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    return structlog.get_logger()


@hydra.main(
    version_base=None,
    config_path=str(pathlib.Path.cwd() / "conf"),
    config_name="config",
)
def cli(cfg: DictConfig) -> None:
    logger = configure_logger(logging.DEBUG if cfg.debug else logging.INFO)

    logger.debug("Configuration", config=cfg)

    conn = duckdb.connect(cfg.duckdb_path)
    conn.execute("SET enable_progress_bar = true")
    conn.install_extension("postgres")
    conn.load_extension("postgres")
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    conn.sql(
        Template(
            """create secret pg_secret (
            type postgres, host '{{cfg.radar.db.host}}',
            port {{cfg.radar.db.port}},
            database '{{cfg.radar.db.database}}',
            user '{{cfg.radar.db.username}}'
            {% if cfg.radar.db.password %}
            , password '{{cfg.radar.db.password}}'
            {% endif %}
        )
        """
        ).render(cfg=cfg)
    )
    conn.sql("ATTACH '' AS pg_db (TYPE postgres, secret pg_secret)")
    conn.sql(f"use pg_db.{cfg.radar.db.schema}")

    conn.install_extension("httpfs")
    conn.load_extension("httpfs")

    conn.sql(
        Template(
            """create secret s3_secret (
            type s3,
            endpoint '{{cfg.radar.bucket.endpoint}}',
            key_id '{{cfg.radar.bucket.access_key}}',
            secret '{{cfg.radar.bucket.secret_key}}',
            url_style '{{cfg.radar.bucket.url_style}}'
        )
        """
        ).render(cfg=cfg)
    )

    for schema_name, schema in cfg.radar.schemas.items():
        if (
            "ignore" in cfg.radar.schemas[schema_name]
            and cfg.radar.schemas[schema_name].ignore
        ):
            logger.info("Skipping schema marked as ignore", schema=schema_name)
            continue
        logger.info("Processing schema", schema=schema_name)

        # Check if tables is configured - require explicit configuration
        if "tables" not in schema:
            logger.info(
                "Skipping schema without tables configuration", schema=schema_name
            )
            continue

        process_all_tables = "*" in schema.tables

        db_tables = conn.execute(
            "SELECT * FROM (SHOW ALL TABLES) WHERE schema = $1", [schema_name]
        ).fetch_arrow_table()

        for table in db_tables.to_pylist():
            table_name = table["name"]

            if process_all_tables:
                # Process all tables from database, use "*" config as default
                # but allow specific table overrides
                if table_name in schema.tables:
                    table_conf = schema.tables[table_name]
                else:
                    table_conf = schema.tables["*"]

            else:
                # Only process explicitly listed tables
                if table_name not in schema.tables:
                    logger.debug("Skipping table not in tables list", table=table_name)
                    continue

                table_conf = schema.tables[table_name]

            # Check if table is marked as ignore
            if table_conf and "ignore" in table_conf and table_conf.ignore:
                logger.debug("Skipping table marked as ignore", table=table_name)
                continue
            target = schema.target if "target" in schema else schema_name

            output_path = f"s3://{cfg.radar.bucket.name}/tables/deployment={cfg.radar.deployment_id}/{target}/{table_name}"

            logger.info(
                "Exporting table",
                table_name=table_name,
                table_conf=table_conf,
                output_path=output_path,
                table=table,
            )

            if table_conf and "raster" in table_conf:
                logger.debug(
                    "using raster export",
                    raster_column=table_conf.raster,
                )
                output_path = f"s3://{cfg.radar.bucket.name}/rasters/deployment={cfg.radar.deployment_id}/{table_conf.target if 'target' in table_conf else target}/{table_name}"  # noqa: E501
                pg_conn_info = PostgresConnectionInfo(
                    host=cfg.radar.db.host,
                    port=cfg.radar.db.port,
                    database=cfg.radar.db.database,
                    username=cfg.radar.db.username,
                    password=cfg.radar.db.get("password"),
                    schema=schema_name,
                )
                s3_conn_info = S3ConnectionInfo(
                    endpoint=cfg.radar.bucket.endpoint,
                    access_key=cfg.radar.bucket.access_key,
                    secret_key=cfg.radar.bucket.secret_key,
                    url_style=cfg.radar.bucket.url_style,
                )
                export_raster_table(
                    pg_conn_info,
                    table_name,
                    output_path + ".tif",
                    logger=logger,
                    s3_conn_info=s3_conn_info,
                    raster_column=table_conf.raster,
                )
            elif table_conf and "date_partition_column" in table_conf:
                geometry_crs = get_geometry_crs(conn, schema_name, table_name)
                if geometry_crs:
                    logger.debug("Detected geometry CRS", crs=geometry_crs)
                logger.debug(
                    "using partitioned export",
                    date_partition_column=table_conf.date_partition_column,
                )
                export_date_partitioned_table(
                    conn,
                    f"{schema_name}.{table_name}",
                    output_path,
                    table_conf.date_partition_column,
                    geometry_crs=geometry_crs,
                    overwrite=cfg.overwrite,
                )
            else:
                geometry_crs = get_geometry_crs(conn, schema_name, table_name)
                if geometry_crs:
                    logger.debug("Detected geometry CRS", crs=geometry_crs)
                export_table(
                    conn,
                    f"{schema_name}.{table_name}",
                    output_path + ".parquet",
                    geometry_crs=geometry_crs,
                    overwrite=cfg.overwrite,
                )
            logger.info("done")


if __name__ == "__main__":
    cli()
