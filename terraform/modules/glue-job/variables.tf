# Required
variable "script_dir" {
  type        = string
  description = "Name of the directory to the folder where the scipt is saved"
}

variable "script_name" {
  type        = string
  description = "Name of the python script to run"
}

variable "resource_bucket" {
  type = object({
    bucket_name = string
    bucket_uri  = string
  })
  description = "The bucket used for the glue jobs temporary directory & scripts"
}

variable "datasets_bucket" {
  type = object({
    bucket_name = string
    bucket_uri  = string
  })
  description = "The bucket used for the glue jobs input and output datasets"
}

# Optional
variable "job_parameters" {
  type        = map(any)
  description = "Optional parameters to pass through to the job"
}

variable "extra_conf" {
  type        = string
  description = "Optional extra configuration to pass - must be prefixed with  --conf "
  default     = ""
}

variable "glue_version" {
  type        = string
  description = "Optional version of glue to use for the job. Defaults to 5.0"
  default     = "5.0"

  validation {
    condition     = contains(["3.0", "4.0", "5.0"], var.glue_version)
    error_message = "Must be one of 3.0, 4.0 or 5.0"
  }
}

variable "worker_type" {
  type        = string
  description = "Optional Glue worker type"
  default     = "G.1X"

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X"], var.worker_type)
    error_message = "Must be one of Standard, G.1X or G.2X"
  }
}

variable "number_of_workers" {
  type        = number
  description = "Optional number of glue workers"
  default     = 2

  validation {
    condition     = var.number_of_workers >= 2
    error_message = "Must be at least 2"
  }
}
