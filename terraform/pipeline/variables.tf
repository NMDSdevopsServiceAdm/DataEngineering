variable "aws_access_key" {
  description = "Access key for AWS - find this in IAM"
  type        = string
  default     = ""
  sensitive   = true
  ephemeral   = true
}
variable "aws_secret_key" {
  description = "Secret key for AWS - displayed when IAM user is created"
  type        = string
  default     = ""
  sensitive   = true
  ephemeral   = true
}

variable "region" {
  default     = "eu-west-2"
  description = "AWS region for data processing"
}

variable "glue_database_name" {
  type        = string
  description = "The name of the glue database"
  default     = "data-engineering-database"
}

# variable "secret_name" {
#   type    = string
#   default = "cqc_api_primary_key"
# }

