variable "dataset_name" {
    type = string
    description = "Raw dataset name"
}

variable "domain_name" {
    type = string
    description = "Domain name of the dataset"
}

variable "pipeline_name" {
    type = string
    description = "Name of the ingest pipeline for the dataset"
}

variable "glue_job_name" {
    type = string
    description = "Name of the glue job for ingesting the dataset"
}