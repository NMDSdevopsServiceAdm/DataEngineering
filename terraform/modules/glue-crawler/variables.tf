# Required

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

# Optional
variable "schedule" {
  description = "An optional schedule in CRON format to execute the crawler"
  type        = string
  default     = null
}

variable "exclusions" {
  description = "An optional list of glob pattern to exclude from the crawl"
  type        = list(string)
  default     = null
}

variable "table_level" {
  description = "Optional level in the S3 path at which to create tables. Default is set to 3 inline with bucket/domain/dataset structrue."
  type        = number
  default     = 3
}

variable "name_postfix" {
  description = "Optional postfix to add to the crawler name"
  type        = string
  default     = ""
}