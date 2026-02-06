variable "aws_access_key" {
  type        = string
  description = "Access key for AWS - find this in IAM"
  default     = ""
  sensitive   = true
  ephemeral   = true
}
variable "aws_secret_key" {
  type        = string
  description = "Secret key for AWS - displayed when IAM user is created"
  default     = ""
  sensitive   = true
  ephemeral   = true
}

variable "region" {
  type        = string
  description = "AWS region for data processing"
  default     = "eu-west-2"
}

variable "glue_database_name" {
  type        = string
  description = "The name of the glue database"
  default     = "data-engineering-database"
}

# variable "secret_name" {
#   type    = string
#   description = "The name of the AWS secret to retreive"
#   default = "cqc_api_primary_key"
# }

