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

variable "bucket" {
  type        = string
  description = "Reference to terraform remote state bucket from *.s3.tfbackend"
}

variable "region" {
  default = "eu-west-2"
}
