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
  description = "Version of glue to use for the job. Default of 2.0"
  type        = string
  default     = "2.0"

  validation {
    condition     = contains(["1.0", "2.0", "3.0"], var.glue_version)
    error_message = "Must be one of 1.0, 2.0 of 3.0"
  }
}
