variable "dataset_for_crawler" {
  description = "The name of the datasets domain for use as the crawler name"
  type        = string
  default     = null
}

variable "glue_role" {
  description = "Glue Role that the crawler will use to execute."
  type = object({
    name = string
    arn  = string
  })
}

variable "data_path" {
  description = "The root path for a given domains data"
  type        = string
  default     = null
}

variable "workspace_glue_database_name" {
  description = "The name of the current workspace glue database"
  type        = string
  default     = null
}

