locals {
  job_name = "${terraform.workspace}-${replace(var.script_name, ".py", "")}_job"
}
