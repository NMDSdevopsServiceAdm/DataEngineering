variable "script_name" {
  description = "Name of the python script to run"
  type        = string
}

variable "glue_role_arn" {
  description = "Glue Role ARN that the job will use to execute."
  type        = string
  default     = null
}

variable "resource_bucket" {
  description = "The bucket used for the glue jobs temporary directory & scripts"
  type = object({
    bucket_name = string
    bucket_uri  = string
  })
}
