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
  description = "DPR dataset version (e.g. 2027.01), used as the version= S3 path segment in the DPR step functions"
  default     = "2027.01"
}

variable "circleci_definition_id" {
  type        = string
  description = "Definition id of the CircleCI pipeline that Publication-Output starts to cut the release tag (CircleCI: Project Settings, Project Setup)"
  default     = ""
}

variable "release_trigger_branch_test" {
  type        = bool
  description = "Off main, makes Publication-Output start the release-tag dry run, for testing the trigger from one branch. Never set on main."
  default     = false
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
