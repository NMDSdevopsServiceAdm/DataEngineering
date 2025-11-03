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

variable "environment" {
  type = list(map(string))
}
