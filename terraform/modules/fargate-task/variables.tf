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
