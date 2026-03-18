# radar-migrate

A tool for migrating PostgreSQL/PostGIS radar data to cloud storage (S3) as Parquet files and Cloud Optimized GeoTIFFs (COG).

## Features

- Export PostgreSQL tables to Parquet files on S3
- Export PostGIS raster tables to Cloud Optimized GeoTIFFs on S3
- Date-partitioned exports for time-series data
- Configurable schema and table selection via Hydra YAML configs
- Support for multiple deployment configurations

## Installation

Requires [pixi](https://pixi.sh/) (recommended) or [uv](https://docs.astral.sh/uv/):

```bash
# With pixi
pixi install
```

## Usage

Run with a radar configuration:

```bash
pixi run radar-migration +radar=<config-name>
```

For example:
```bash
pixi run radar-migration +radar=flex-sommer-2020
pixi run radar-migration +radar=merlin-bremanger-2016
```

### Command-line options

Override configuration values:
```bash
pixi run radar-migration +radar=flex-sommer-2020 overwrite=false debug=false
```

## Configuration

Configuration files are in `conf/radar/`. Each deployment needs a YAML file.

### Structure

```yaml
defaults:
  - base        # Base defaults

db:
  database: my_database
  schema: public

deployment_id: my-deployment

schemas:
  my_schema:
    target: raw           # Output folder name (default: schema name)
    tables:
      "*":                # Export all tables (can still override specific ones)
      specific_table:     # Override config for this table
        date_partition_column: timestamp
    # OR list only specific tables:
    tables:
      table1:             # No special config
      table2:
        ignore: true      # Skip this table
      table3:
        date_partition_column: timestamp  # Partition by date
      raster_table:
        raster: rast      # Export as COG (column name)
```

### Schema options

| Option | Description |
|--------|-------------|
| `ignore: true` | Skip the entire schema |
| `target` | Output folder name (defaults to schema name) |
| `tables: {"*": ...}` | Export all tables in schema |
| `tables: {...}` | Export only listed tables |

### Table options

| Option | Description |
|--------|-------------|
| `ignore: true` | Skip this table |
| `date_partition_column` | Partition Parquet by this date column |
| `raster` | Export as COG using this raster column name |

### Database config

```yaml
db:
  host: localhost
  port: 5432
  database: mydb
  username: user
  password: pass    # Optional
  schema: public    # Default schema for DuckDB connection
```

### S3 bucket config

```yaml
bucket:
  name: my-bucket
  endpoint: s3.example.com
  access_key: ACCESS_KEY
  secret_key: SECRET_KEY
  url_style: path   # or "virtual"
```

## Output structure

Files are written to S3 with the following structure:

```
s3://<bucket>/tables/deployment=<deployment_id>/<target>/<table>.parquet
s3://<bucket>/rasters/deployment=<deployment_id>/<target>/<table>.tif
```

Date-partitioned tables:
```
s3://<bucket>/tables/deployment=<deployment_id>/<target>/<table>/<column>_year=YYYY/<column>_month=MM/<column>_day=DD/
```

## Development

```bash
pixi run radar-migration +radar=<config>
```
```bash
pre-commit install
```

To run pre-commit on all files:
```bash
pre-commit run --all-files
```

### How to install a package
Run `uv add <package-name>` to install a package. For example:
```bash
uv add requests
```

#### Visual studio code
If you are using visual studio code install the recommended extensions


### Tools installed
- pixi
- pre-commit (optional)
