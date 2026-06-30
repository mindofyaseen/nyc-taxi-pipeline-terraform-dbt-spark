# NYC Taxi Data Pipeline — AWS + Snowflake

End-to-end data engineering project built as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

> **Stack:** AWS S3 · Snowflake · dbt · Apache Spark · Terraform

---

## What This Project Does

Builds a complete data pipeline for NYC Yellow and Green taxi trip data:

1. **Ingestion** — Downloads CSV files from GitHub, converts to Parquet, uploads to S3
2. **Warehousing** — Loads Parquet from S3 into Snowflake (58.5M rows)
3. **Transformation** — dbt models clean and aggregate the data
4. **Batch Processing** — Spark reads from S3, computes monthly revenue by zone, writes results back to S3

---

## Architecture

```
NYC Taxi CSVs (GitHub)
        │
        ▼  Python (pandas + boto3)
AWS S3  ──────────────────────────────────────────┐
dezoomcamp-data-lake-ym/                          │
├── green/   (24 files, 7.8M rows)                │ Spark S3A
├── yellow/  (7 files, 50.8M rows)                │
└── reports/ (aggregated output)  ◄───────────────┘
        │
        │  COPY INTO
        ▼
Snowflake (DEZOOMCAMP.NYTAXI)
├── green_tripdata
└── yellow_tripdata
        │
        │  dbt-snowflake
        ▼
dbt Models (DEZOOMCAMP.dbt_yaseen)
├── stg_green_tripdata
├── stg_yellow_tripdata
├── fact_trips
├── dim_zones
└── fct_monthly_zone_revenue
```

See [docs/architecture.md](docs/architecture.md) for the full diagram.

---

## Project Structure

```
dezoomcamp-aws-snowflake/
├── terraform/              # IaC — provisions S3 + Snowflake
│   ├── main.tf
│   └── variables.tf
├── dbt/                    # Analytics engineering
│   ├── models/             # 8 models (staging → intermediate → marts)
│   ├── macros/
│   ├── seeds/              # taxi_zone_lookup, payment_type_lookup
│   └── dbt_project.yml
├── spark/                  # Batch processing
│   └── 06_spark_sql_s3.py  # Revenue aggregation via S3A
├── scripts/                # Data loading
│   ├── load_to_s3.py       # CSV → Parquet → S3
│   └── setup_snowflake.py  # S3 → Snowflake COPY INTO
└── docs/
    ├── architecture.md     # Full architecture diagram
    └── setup_guide.md      # Step-by-step setup
```

---

## Results

| Component | Result |
|-----------|--------|
| S3 files uploaded | 31 Parquet files |
| Total rows in Snowflake | 58,547,656 |
| dbt models | 8 / 8 passed |
| dbt tests | 33 / 33 passed |
| Spark job | Reads S3 → aggregates → writes S3 |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Cloud Storage | AWS S3 (us-east-1) |
| Data Warehouse | Snowflake (DEZOOMCAMP.NYTAXI) |
| Infrastructure | Terraform (aws + Snowflake-Labs providers) |
| Transformation | dbt-snowflake 1.11.6 |
| Batch Processing | Apache Spark 4.1.1 + hadoop-aws 3.4.2 |
| Language | Python 3.12 |

---

## Security

- No credentials hardcoded anywhere
- Snowflake password: `~/.dezoomcamp_password` (gitignored, outside repo)
- AWS credentials: `~/.aws/credentials` (AWS CLI standard)
- dbt profiles: `~/.dbt/profiles.yml` (outside repo)
- Terraform state: gitignored

---

## Setup

See [docs/setup_guide.md](docs/setup_guide.md) for full instructions.
