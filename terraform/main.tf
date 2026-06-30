terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Reads SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD from environment variables
provider "snowflake" {
  role = "SYSADMIN"
}

# ── S3 Data Lake (equivalent to GCS bucket) ──────────────────────────────────

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_rules" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    id     = "delete_objects_older_than_30_days"
    status = "Enabled"

    expiration {
      days = 30
    }
    filter {
      prefix = ""
    }
  }
}

# ── Snowflake Data Warehouse (equivalent to BigQuery dataset) ─────────────────

resource "snowflake_database" "dezoomcamp" {
  name = var.snowflake_database
}

resource "snowflake_schema" "nytaxi" {
  database = snowflake_database.dezoomcamp.name
  name     = var.snowflake_schema
}

resource "snowflake_warehouse" "compute_wh" {
  name           = var.snowflake_warehouse
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
}
