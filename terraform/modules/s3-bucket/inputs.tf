variable "bucket_name" {
  description = "Name of the python script to run"
  type        = string
}

variable "empty_bucket_on_destroy" {
  description = "Should the bucket be emptied before destroying?"
  type        = bool
  default     = false
}
