variable "task_name" {
  description = "Name for the ECS Fargate, used in resource naming and tags."
  type        = string
}

variable "region" {
  default = "eu-west-2"
  type    = string
}

variable "ecr_repo_name" {
  type    = string
  default = "fargate/cqc"
}

variable "secret_name" {
  type    = string
  default = "cqc_api_primary_key"
}

variable "cluster_arn" {
  type = string
}

variable "tag_name" {
  type = string
}

variable "cpu_size" {
  type    = number
  default = 8192
}

variable "ram_size" {
  type    = number
  default = 61440
}

# Expects ephemeral_storage.0.size_in_gib to be in the range (21 - 200)
variable "ephemeral_storage_size" {
  type    = number
  default = 21
}

variable "environment" {
  type = list(map(string))
}

variable "raw_bucket_arn" {
  description = "ARN of the raw data landing bucket to grant this task read-only access to. Omit for tasks that don't read raw data."
  type        = string
  default     = null
}

variable "grant_archive_promoted_read" {
  description = "Whether to grant this task read-only access to the promoted archive bucket. Separate from the ARN because count can't depend on a value unknown until apply."
  type        = bool
  default     = false
}

variable "archive_promoted_bucket_arn" {
  description = "ARN of the long-term promoted archive bucket. Only used when grant_archive_promoted_read is true."
  type        = string
  default     = null
}
