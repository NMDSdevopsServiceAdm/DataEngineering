variable "state_machine_arn" {
  type        = string
  description = "The arn of the state machine to schedule"
}

variable "state_machine_name" {
  type        = string
  description = "The name of the state machine to schedule"
}

variable "schedule_description" {
  type        = string
  description = "A description of the schedule applied to the state machine"
}

variable "schedule_expression" {
  type        = string
  description = "The schedule applied to the state machine"
}