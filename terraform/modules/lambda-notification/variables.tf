variable "lambda_name" {
  type        = string
  description = "Name of the lambda (needs to match the folder in the lambdas folder for filepath generation)"
}

variable "resource_bucket" {
  type        = string
  description = "Name of the pipeline resources bucket containing the lambda"
}

variable "sns_topic_arn" {
  type        = string
  description = "ARN of the SNS topic for the lambda"
}