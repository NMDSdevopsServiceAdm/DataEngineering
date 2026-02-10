variable "task_name" {
  type        = string
  description = "Name for the ECS Fargate, used in resource naming and tags."
}

variable "region" {
  type        = string
  description = "AWS region for data processing"
  default     = "eu-west-2"
}

variable "ecr_repo_name" {
  type        = string
  description = "Directory for the ECR repository in AWS"
  default     = "fargate/cqc"
}

variable "secret_name" {
  type        = string
  description = "The name of secret stored in AWS for retrieval"
  default     = "cqc_api_primary_key"
}

variable "tag_name" {
  type        = string
  description = "Tag for ECR Repository"
}

variable "cpu_size" {
  type        = number
  description = "CPU size for ECS cluster"
  default     = 8192
}

variable "ram_size" {
  type        = number
  description = "RAM size for ECS cluster"
  default     = 61440
}

variable "environment" {
  type        = list(map(string))
  description = "Environment variables for ECS container"
}
