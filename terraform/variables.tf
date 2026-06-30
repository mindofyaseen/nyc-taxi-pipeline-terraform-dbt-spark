variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the data lake"
  type        = string
  default     = "dezoomcamp-data-lake-ym"
}

variable "snowflake_database" {
  description = "Snowflake database name (equivalent to BigQuery project/dataset)"
  type        = string
  default     = "DEZOOMCAMP"
}

variable "snowflake_schema" {
  description = "Snowflake schema name (equivalent to BigQuery dataset)"
  type        = string
  default     = "NYTAXI"
}

variable "snowflake_warehouse" {
  description = "Snowflake virtual warehouse name"
  type        = string
  default     = "COMPUTE_WH"
}
