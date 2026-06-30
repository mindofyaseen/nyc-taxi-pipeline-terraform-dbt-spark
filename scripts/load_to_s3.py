"""
NYC Taxi Data Pipeline — Download CSV from GitHub, convert to Parquet, upload to S3.
Run this before dbt or Spark.

Usage:
    pip install pandas boto3 pyarrow
    python scripts/load_to_s3.py
"""
import io
import boto3
import pandas as pd
import urllib.request

BUCKET  = "dezoomcamp-data-lake-ym"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

DATASETS = {
    "green":  [(2019, m) for m in range(1, 13)] + [(2020, m) for m in range(1, 13)],
    "yellow": [(2019, m) for m in range(1, 8)],
}

s3 = boto3.client("s3", region_name="us-east-1")


def already_uploaded(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


for service, months in DATASETS.items():
    for year, month in months:
        filename = f"{service}_tripdata_{year}-{month:02d}.csv.gz"
        s3_key   = f"{service}/{service}_tripdata_{year}-{month:02d}.parquet"

        if already_uploaded(s3_key):
            print(f"  SKIP  {s3_key}")
            continue

        url = f"{BASE_URL}/{service}/{filename}"
        print(f"  Downloading {filename}...")
        try:
            with urllib.request.urlopen(url) as r:
                df = pd.read_csv(r, compression="gzip", low_memory=False)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            s3.upload_fileobj(buf, BUCKET, s3_key)
            print(f"  Uploaded  {s3_key}  ({len(df):,} rows)")
        except Exception as e:
            print(f"  ERROR {filename}: {e}")
