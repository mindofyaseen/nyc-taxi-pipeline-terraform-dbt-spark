# Setup Guide

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Data loading scripts |
| AWS CLI | 2.x | S3 access |
| Terraform | 1.x | Infrastructure |
| Java | 17 | Spark |
| Spark | 4.1.1 | Batch processing |
| dbt-snowflake | 1.11.6 | Transformations |

---

## Step 1 — Credentials Setup

**Never hardcode credentials. Store them outside the repo.**

```bash
# Snowflake password
echo "YourSnowflakePassword" > ~/.dezoomcamp_password

# Set env vars (add to ~/.bashrc or ~/.zshrc)
export SNOWFLAKE_PASSWORD=$(cat ~/.dezoomcamp_password)
export SNOWFLAKE_ACCOUNT=FUEATEX-AR79928
export SNOWFLAKE_USER=YASEEN

# AWS credentials (already configured via AWS CLI)
aws configure
```

---

## Step 2 — Terraform (Provision Infrastructure)

```bash
cd terraform/

# Set Snowflake env vars first
export SNOWFLAKE_ACCOUNT=FUEATEX-AR79928
export SNOWFLAKE_USER=YASEEN
export SNOWFLAKE_PASSWORD=$(cat ~/.dezoomcamp_password)

terraform init
terraform plan
terraform apply
```

**Creates:**
- S3 bucket: `dezoomcamp-data-lake-ym` (us-east-1)
- Snowflake DB: `DEZOOMCAMP`
- Snowflake Schema: `NYTAXI`
- Snowflake Warehouse: `COMPUTE_WH` (X-SMALL)

---

## Step 3 — Load Data to S3 + Snowflake

```bash
pip install pandas boto3 pyarrow snowflake-connector-python

# Download NYC taxi CSVs → convert to Parquet → upload to S3
python scripts/load_to_s3.py

# Create Snowflake tables + COPY INTO from S3
python scripts/setup_snowflake.py
```

---

## Step 4 — dbt (Analytics Engineering)

**Create `~/.dbt/profiles.yml`** (outside repo):

```yaml
taxi_rides_ny:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "FUEATEX-AR79928"
      user: "YASEEN"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: DEZOOMCAMP
      warehouse: COMPUTE_WH
      schema: dbt_yaseen
      threads: 4
    prod:
      type: snowflake
      account: "FUEATEX-AR79928"
      user: "YASEEN"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: DEZOOMCAMP
      warehouse: COMPUTE_WH
      schema: NYTAXI
      threads: 4
```

```bash
cd dbt/
pip install dbt-snowflake

export SNOWFLAKE_PASSWORD=$(cat ~/.dezoomcamp_password)

dbt deps
dbt seed
dbt run
dbt test
```

**Expected output:** 8/8 models ✅ | 33/33 tests ✅

---

## Step 5 — Spark (Batch Processing)

**On Windows, set HADOOP_HOME:**
```powershell
$env:HADOOP_HOME = "C:\path\to\hadoop"   # needs winutils.exe
$env:SPARK_HOME  = "C:\path\to\spark-4.1.1"
```

```bash
spark-submit \
  --packages "org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.787" \
  --conf "spark.driver.host=localhost" \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.hadoop.fs.s3a.analytics.accelerator.enabled=false" \
  spark/06_spark_sql_s3.py \
    --input_green=s3a://dezoomcamp-data-lake-ym/green/ \
    --input_yellow=s3a://dezoomcamp-data-lake-ym/yellow/ \
    --output=s3a://dezoomcamp-data-lake-ym/reports/revenue-2019
```

**Output:** Parquet files at `s3://dezoomcamp-data-lake-ym/reports/revenue-2019/`
