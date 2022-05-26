resource "aws_glue_trigger" "glue_job_trigger" {
  count    = terraform.workspace == "main" && var.trigger ? 1 : 0
  name     = "${terraform.workspace}-${var.script_name}-trigger"
  schedule = var.trigger_schedule
  type     = "SCHEDULED"

  actions {
    job_name = local.job_name
  }
}
