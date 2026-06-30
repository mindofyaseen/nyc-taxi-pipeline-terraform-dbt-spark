"""
Creates Snowflake tables and loads data from S3.
Run after load_to_s3.py.

Usage:
    pip install snowflake-connector-python boto3
    python scripts/setup_snowflake.py
"""
from pathlib import Path
import boto3
import snowflake.connector

# Read password from local file (never hardcode)
SF_PASSWORD = (Path.home() / ".dezoomcamp_password").read_text().strip()

session = boto3.Session()
creds   = session.get_credentials().get_frozen_credentials()

BUCKET      = "dezoomcamp-data-lake-ym"
SF_ACCOUNT  = "FUEATEX-AR79928"
SF_USER     = "YASEEN"
SF_DATABASE = "DEZOOMCAMP"
SF_SCHEMA   = "NYTAXI"
SF_WH       = "COMPUTE_WH"

conn = snowflake.connector.connect(
    account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
    warehouse=SF_WH, database=SF_DATABASE, schema=SF_SCHEMA, role="SYSADMIN"
)
cur = conn.cursor()

print("Creating S3 stage...")
cur.execute(f"""
    CREATE OR REPLACE STAGE {SF_DATABASE}.{SF_SCHEMA}.s3_taxi_stage
      URL='s3://{BUCKET}/'
      CREDENTIALS=(AWS_KEY_ID='{creds.access_key}' AWS_SECRET_KEY='{creds.secret_key}')
      FILE_FORMAT=(TYPE='PARQUET');
""")

for table, ts_col, prefix in [
    ("green_tripdata",  "lpep_pickup_datetime",  "green"),
    ("yellow_tripdata", "tpep_pickup_datetime",   "yellow"),
]:
    print(f"Loading {table}...")
    cur.execute(f"""
        COPY INTO {SF_DATABASE}.{SF_SCHEMA}.{table}
        FROM @{SF_DATABASE}.{SF_SCHEMA}.s3_taxi_stage/{prefix}/
        FILE_FORMAT=(TYPE='PARQUET')
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        PURGE=FALSE ON_ERROR=CONTINUE;
    """)
    cur.execute(f"SELECT COUNT(*) FROM {SF_DATABASE}.{SF_SCHEMA}.{table}")
    print(f"  {table}: {cur.fetchone()[0]:,} rows")

cur.close()
conn.close()
print("Done.")
