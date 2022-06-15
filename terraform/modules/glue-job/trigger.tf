resource "aws_glue_trigger" "glue_job_trigger" {
  count    = terraform.workspace == "main" && var.trigger ? 1 : 0
  name     = local.trigger_name
  schedule = var.trigger_schedule
  type     = "SCHEDULED"

  actions {
    job_name = local.job_name
  }
}
