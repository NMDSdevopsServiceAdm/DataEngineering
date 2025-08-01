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
  default = "869-dev/cqc"
}

variable "secret_name" {
  type    = string
  default = "cqc_api_primary_key"
}

