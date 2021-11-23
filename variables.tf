variable "bucket_name" {
  default = "sfc-data-engineering"
}

variable "ascwds_data_location" {
  default = "s3://sfc-data-engineering/domain=ASCWDS/"
}

variable "acl_value" {
  default = "private"
}

variable "aws_access_key" {
  description = "Access key for AWS - find this in IAM"
  type        = string
  sensitive   = true
}
variable "aws_secret_key" {
  description = "Secret key for AWS - displayed when IAM user is created"
  type        = string
  sensitive   = true
}

variable "region" {
  default = "eu-west-2"
}

variable "glue_db_name" {
  default = "data_engineering_glue_db"
}

variable "glue_db_crawler_name" {
  default = "data_engineering_glue_crawler"
}

variable "git_repo_url" {
  default = "https://github.com/NMDSdevopsServiceAdm/DataEngineering.git"
}

variable "scripts_location" {
  default = "s3://sfc-data-engineering/scripts/"
}

variable "glue_temp_dir" {
  default = "s3://sfc-data-engineering/temp/"
}
