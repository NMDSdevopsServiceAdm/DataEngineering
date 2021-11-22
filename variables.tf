variable "bucket_name" {
  default = "sfc-data-engineering"
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

variable "glue_iam_role" {
  default = "arn:aws:iam::344210435447:role/SfCGlueRole"
}

variable "git_repo_url" {
  default = "https://github.com/NMDSdevopsServiceAdm/DataEngineering.git"
}
