# Required
variable "script_dir" {
  description = "Name of the directory to the folder where the scipt is saved"
  type        = string
}

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

# Optional
variable "job_parameters" {
  description = "Optional parameters to pass through to the job"
  type        = map(any)
}

variable "extra_conf" {
  description = "Optional extra configuration to pass - must be prefixed with  --conf "
  type        = string
  default     = ""
}

variable "glue_version" {
  description = "Optional version of glue to use for the job. Defaults to 5.0"
  type        = string
  default     = "5.0"

  validation {
    condition     = contains(["3.0", "4.0", "5.0"], var.glue_version)
    error_message = "Must be one of 3.0, 4.0 or 5.0"
  }
}

variable "worker_type" {
  description = "Optional Glue worker type"
  default     = "G.1X"
  type        = string

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X"], var.worker_type)
    error_message = "Must be one of Standard, G.1X or G.2X"
  }
}

variable "number_of_workers" {
  description = "Optional number of glue workers"
  default     = 2
  type        = number

  validation {
    condition     = var.number_of_workers >= 2
    error_message = "Must be at least 2"
  }
}
