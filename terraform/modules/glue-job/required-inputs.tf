variable "script_name" {
  description = "Name of the python script to run"
  type        = string
}

variable "glue_role" {
  description = "Glue Role that the job will use to execute."
  type = object({
    name = string
    arn  = string
  })
}
variable "resource_bucket" {
  description = "The bucket used for the glue jobs temporary directory & scripts"
  type = object({
    bucket_name = string
    bucket_uri  = string
  })
}

variable "datasets_bucket" {
  description = "The bucket used for the glue jobs input and output datasets"
  type = object({
    bucket_name = string
    bucket_uri  = string
  })
}
