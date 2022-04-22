variable "script_name" {
  description = "Name of the python script to run"
  type        = string
}

variable "glue_role_arn" {
  description = "Glue Role ARN that the job will use to execute."
  type        = string
  default     = null
}

variable "resource_bucket_uri" {
  description = "The base uri of the bucket used for the glue jobs temporary directory & scripts"
  type        = string
  default     = null
}