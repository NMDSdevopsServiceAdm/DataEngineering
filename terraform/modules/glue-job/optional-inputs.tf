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

