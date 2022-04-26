variable "datset_for_crawler" {
  description = "The name of the datasets domain for use as the crawler name"
  type        = string
  default     = null
}

variable "glue_crawler_iam_role" {
  description = "Role Arn for the glue crawler to assume during execution"
  type        = string
  default     = null
}

variable "data_path" {
  description = "The root path for a given domains data"
  type        = string
  default     = null
}