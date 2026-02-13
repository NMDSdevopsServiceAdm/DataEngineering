variable "pipeline_name" {
  type        = string
  description = "Name of the step function"
}

variable "definition" {
  type        = string
  description = "Definition of the step function using a template file and variable list"
}

variable "dataset_bucket_name" {
  type        = string
  description = "Name of datasets bucket in s3"
}

