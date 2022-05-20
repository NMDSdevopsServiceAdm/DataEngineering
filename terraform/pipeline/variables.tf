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

variable "ascwds_root_data_location" {
  type        = string
  default     = "s3://sfc-data-engineering/domain=ASCWDS/"
  description = "Root directory for all data sourced from the ASCWDS domain"
}

variable "data_engineering_root_data_location" {
  type        = string
  default     = "s3://sfc-data-engineering/domain=data_engineering/"
  description = "Root directory for all data generated in the Data Engineering domain"
}
variable "cqc_root_data_location" {
  type        = string
  default     = "s3://sfc-data-engineering/domain=CQC/"
  description = "Root directory for all data sourced from the CQC domain"
}

variable "glue_database_name" {
  type        = string
  description = "The name of the glue database"
  default     = "data-engineering-database"
}