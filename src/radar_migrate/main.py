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

from radar_migrate.export import export_date_partitioned_table, export_table

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
    conn.sql("ATTACH '' AS pg (TYPE postgres, secret pg_secret); use pg;")

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
        logger.info("Processing schema", schema=schema_name)
        tables = conn.execute(
            "SELECT * FROM (SHOW ALL TABLES) WHERE schema = $1", [schema_name]
        ).fetch_arrow_table()
        for table in tables.to_pylist():
            if table["name"] not in list(map(lambda t: t, schema.tables.keys())):
                # logger.debug("Skipping table not in config", table=table["name"])
                continue

            table_conf = schema.tables[table["name"]]
            target = schema.target if "target" in schema else schema_name

            output_path = f"s3://{cfg.radar.bucket.name}/tables/deployment={cfg.radar.deployment_id}/{target}/{table['name']}"
            logger.info("Exporting table", table=table, output_path=output_path)

            if table_conf and "date_partition_column" in table_conf:
                logger.debug(
                    "using partitioned export",
                    date_partition_column=table_conf.date_partition_column,
                )
                export_date_partitioned_table(
                    conn,
                    f"{schema_name}.{table['name']}",
                    output_path,
                    table_conf.date_partition_column,
                    overwrite=cfg.overwrite,
                )
            else:
                export_table(
                    conn,
                    f"{schema_name}.{table['name']}",
                    output_path + ".parquet",
                    overwrite=cfg.overwrite,
                )
            logger.info("done")


if __name__ == "__main__":
    cli()
