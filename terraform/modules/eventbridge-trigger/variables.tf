variable "dataset_name" {
    type = string
    description = "Raw dataset name"
}

variable "domain_name" {
    type = string
    description = "Domain name of the dataset"
}

variable "state_machine_arn" {
    type = string
    description = "The arn of the state machine to schedule"
}

variable "state_machine_name" {
    type = string
    description = "The name of the state machine to schedule"
}

variable "glue_job_name" {
    type = string
    description = "Name of the glue job for ingesting the dataset"
}