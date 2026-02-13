variable "bucket_name" {
  type        = string
  description = "Name of the S3 bucket"
}

variable "empty_bucket_on_destroy" {
  type        = bool
  description = "Should the bucket be emptied before destroying?"
  default     = false
}
