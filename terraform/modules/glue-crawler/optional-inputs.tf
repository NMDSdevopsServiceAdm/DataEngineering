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
  description = "Level in the S3 path at which to create tables. Default is set to 3 inline with bucket/domain/dataset structrue."
  type        = number
  default     = 3
}

variable "name_postfix" {
  description = "Optional postfix to add to the crawler name"
  type        = string
  default     = ""
}