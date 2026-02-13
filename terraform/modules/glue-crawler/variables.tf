# Required

variable "dataset_for_crawler" {
  type        = string
  description = "The name of the datasets domain for use as the crawler name"
  default     = null
}

variable "data_path" {
  type        = string
  description = "The root path for a given domains data"
  default     = null
}

variable "workspace_glue_database_name" {
  type        = string
  description = "The name of the current workspace glue database"
  default     = null
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
variable "schedule" {
  type        = string
  description = "An optional schedule in CRON format to execute the crawler"
  default     = null
}

variable "exclusions" {
  type        = list(string)
  description = "An optional list of glob pattern to exclude from the crawl"
  default     = null
}

variable "table_level" {
  type        = number
  description = "Optional level in the S3 path at which to create tables. Default is set to 3 inline with bucket/domain/dataset structrue."
  default     = 3
}

variable "name_postfix" {
  type        = string
  description = "Optional postfix to add to the crawler name"
  default     = ""
}