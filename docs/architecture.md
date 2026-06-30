# Architecture — NYC Taxi Data Pipeline (AWS + Snowflake)

## High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│          NYC Taxi CSV files (DataTalksClub GitHub)                  │
│          Green: 2019-2020  |  Yellow: 2019 (Jan-Jul)               │
└────────────────────────────┬────────────────────────────────────────┘
                             │ Python (pandas + boto3)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      DATA LAKE — AWS S3                              │
│              Bucket: dezoomcamp-data-lake-ym (us-east-1)            │
│                                                                      │
│   s3://dezoomcamp-data-lake-ym/                                     │
│   ├── green/    (24 Parquet files — 7.8M rows)                     │
│   ├── yellow/   (7 Parquet files  — 50.8M rows)                    │
│   └── reports/  (Spark output — revenue aggregations)              │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │ Snowflake COPY INTO           │ Spark S3A
               │ (MATCH_BY_COLUMN_NAME)        │ (hadoop-aws 3.4.2)
               ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────────────┐
│  DATA WAREHOUSE          │    │  BATCH PROCESSING — Spark 4.1.1      │
│  Snowflake               │    │                                       │
│  Account: FUEATEX-AR79928│    │  Input:  s3a://…/green/ + /yellow/  │
│  DB: DEZOOMCAMP          │    │  Output: s3a://…/reports/revenue-…   │
│  Schema: NYTAXI          │    │                                       │
│  WH: COMPUTE_WH (XS)    │    │  Aggregation: monthly revenue        │
│                          │    │  by pickup zone + service type       │
│  Tables:                 │    └──────────────────────────────────────┘
│  ├── green_tripdata      │
│  └── yellow_tripdata     │
└──────────────┬───────────┘
               │ dbt-snowflake
               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  ANALYTICS — dbt (taxi_rides_ny)                     │
│                                                                      │
│  Staging         →  Intermediate    →  Marts                        │
│  ┌────────────┐     ┌────────────┐     ┌──────────────────────┐    │
│  │stg_green   │     │int_trips   │     │fact_trips            │    │
│  │stg_yellow  │  →  │int_trips   │  →  │dim_zones             │    │
│  └────────────┘     │_unioned    │     │dim_vendors           │    │
│                     └────────────┘     │fct_monthly_zone      │    │
│  Seeds:                                │_revenue              │    │
│  ├── taxi_zone_lookup                  └──────────────────────┘    │
│  └── payment_type_lookup                                            │
│                                                                      │
│  8 models  |  2 seeds  |  33 tests  |  All passing ✅              │
└─────────────────────────────────────────────────────────────────────┘
```

## Infrastructure (Terraform)

```
Terraform
├── Provider: hashicorp/aws        5.100.0
├── Provider: Snowflake-Labs/snowflake  0.100.0
│
├── aws_s3_bucket                  dezoomcamp-data-lake-ym
├── aws_s3_bucket_versioning       enabled
├── aws_s3_bucket_public_access_block
├── aws_s3_bucket_lifecycle_configuration  (90-day expiry on tmp/)
│
├── snowflake_database             DEZOOMCAMP
├── snowflake_schema               NYTAXI
└── snowflake_warehouse            COMPUTE_WH (X-SMALL, auto-suspend 60s)
```

## Service Mapping (GCP → AWS + Snowflake)

| Original (GCP)         | Replacement                        |
|------------------------|------------------------------------|
| Google Cloud Storage   | AWS S3                             |
| BigQuery               | Snowflake                          |
| Kestra GCP plugins     | Kestra AWS S3 + JDBC Snowflake     |
| dbt-bigquery           | dbt-snowflake                      |
| Spark + DataProc       | Spark 4.1.1 + hadoop-aws (local)   |
| Terraform google       | Terraform aws + snowflake          |
| `gs://` URIs           | `s3a://dezoomcamp-data-lake-ym/`   |

## Data Volume

| Dataset       | Files | Rows       | Size (approx) |
|---------------|-------|------------|---------------|
| Green taxi    | 24    | 7,778,101  | ~600 MB       |
| Yellow taxi   | 7     | 50,769,555 | ~2.5 GB       |
| **Total**     | **31**| **58.5M**  | **~3.1 GB**   |
