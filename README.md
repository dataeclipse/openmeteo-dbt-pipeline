# openmeteo-dbt-pipeline

Minimal **batch data pipeline**: ingest daily city weather from the [Open-Meteo Historical API](https://open-meteo.com/en/docs/historical-api) (no API key) into **Hive-style partitioned Parquet**, then model with **dbt** and **DuckDB**.

Good for portfolios and internships: clear grain, idempotent daily files, easy to explain in interviews.

## Features

- **Extract:** Python + `urllib` → one Parquet file per UTC day under `data/raw/weather_city_daily/dt=YYYY-MM-DD/cities.parquet`
- **Load / transform:** dbt staging + mart on top of `read_parquet(..., hive_partitioning=true)`
- **Quality:** basic `not_null` tests in `dbt_project/models/schema.yml`
- **Cities:** Moscow, Berlin, London, New York, Tokyo (fixed list in `pipelines/ingest_weather_daily.py`)

## Why Open-Meteo / weather?

Practical default for a public repo: **no secrets**, **stable HTTP**, **one row per city per day** (fits `dt=` partitions and idempotent reruns), **no PII**, safe for CI demos.

## Requirements

- **Python 3.11+** (3.12 recommended)
- Internet access for the first ingest step

## Quick start

Clone the repo and run from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-tools.txt
mkdir -p data/duckdb               # Windows: mkdir data\duckdb  (if needed)
python pipelines/ingest_weather_daily.py
python -m dbt.cli.main build --project-dir dbt_project --profiles-dir dbt_project
```

Ingest **yesterday (UTC)** by default. For a specific day:

```bash
python pipelines/ingest_weather_daily.py --date 2026-03-29
```

### dbt not on `PATH`

Use the module form (works with the Windows Store Python layout too):

```bash
python -m dbt.cli.main build --project-dir dbt_project --profiles-dir dbt_project
```

## Project layout

```
pipelines/
  ingest_weather_daily.py    # API → Parquet
dbt_project/
  models/
    staging/stg_weather_city_daily.sql
    marts/mart_weather_city_daily.sql
    schema.yml               # tests
  dbt_project.yml
  profiles.yml               # DuckDB path: data/duckdb/openmeteo.duckdb
requirements-tools.txt
```

Generated data and the DuckDB file are ignored by git (see `.gitignore`).

## License

Educational / portfolio use. Add a `LICENSE` file if you need a standard OSS license.
