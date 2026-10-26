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

variable "commit_sha" {
  type        = string
  description = "Commit sha of the code being deployed, recorded in the archive run log"
  default     = "unknown"
}

variable "dpr_version" {
  type        = string
  description = "DPR dataset version (e.g. 2027.01), used as the version= S3 path segment across the DPR ingest/merge/estimate pipeline"
  default     = "2027.01"
}

variable "region" {
  default = "eu-west-2"
}

variable "glue_database_name" {
  type        = string
  description = "The name of the glue database"
  default     = "data-engineering-database"
}

variable "secret_name" {
  type    = string
  default = "cqc_api_primary_key"
}
