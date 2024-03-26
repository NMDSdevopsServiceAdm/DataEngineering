variable "job_parameters" {
  description = "Parameters to pass through to the job"
  type        = map(any)
}

variable "trigger" {
  description = "Should a trigger be added to this job?"
  type        = bool
  default     = false
}

variable "trigger_schedule" {
  description = "Cron schedule for triggering job. Example: 'cron(00 07 * * ? *)'"
  type        = string
  default     = ""
}

variable "glue_version" {
  description = "Version of glue to use for the job. Defaults to 3.0"
  type        = string
  default     = "3.0"

  validation {
    condition     = contains(["3.0", "4.0"], var.glue_version)
    error_message = "Must be one of 3.0 or 4.0"
  }
}

variable "worker_type" {
  description = "Glue worker type"
  default     = "Standard"
  type        = string

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X"], var.worker_type)
    error_message = "Must be one of Standard, G.1X or G.2X"
  }
}

variable "number_of_workers" {
  description = "Number of glue workers"
  default     = 2
  type        = number

  validation {
    condition     = var.number_of_workers >= 2
    error_message = "Must be at least 2"
  }
}
