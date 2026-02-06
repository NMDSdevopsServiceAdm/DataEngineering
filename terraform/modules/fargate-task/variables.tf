variable "task_name" {
  description = "Name for the ECS Fargate, used in resource naming and tags."
  type        = string
}

variable "region" {
  default = "eu-west-2"
  type    = string
  description = "AWS region for data processing"
}

variable "ecr_repo_name" {
  type = string
  default = "fargate/cqc"
  description = "Directory for the ECR repository in AWS"
}

variable "secret_name" {
  type    = string
  default = "cqc_api_primary_key"
  description = "The name of secret stored in AWS for retrieval"
}

variable "cluster_arn" {
  type = string
  description = "The ARN of the ECS cluster"
}

variable "tag_name" {
  type = string
  description = "Tag for ECR Repository"
}

variable "cpu_size" {
  type    = number
  default = 8192
  description = "CPU size for ECS cluster"
}

variable "ram_size" {
  type    = number
  default = 61440
  description = "RAM size for ECS cluster"
}

variable "environment" {
  type = list(map(string))
  description = "Environment variables for ECS container"
}
