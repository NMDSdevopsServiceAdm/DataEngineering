locals {
  # Maps StepFunction files in step-functions/dynamic using filenames as keys
  step_functions = tomap({
    for fn in fileset("step-functions/dynamic", "*.json") :
    substr(fn, 0, length(fn) - 5) => "step-functions/dynamic/${fn}"
  })
}



# Created explicitly as required by dynamic step functions
resource "aws_sfn_state_machine" "step_function" {
  name       = "${local.workspace_prefix}-${var.pipeline_name}"
  role_arn   = aws_iam_role.step_function_iam_role.arn
  type       = "STANDARD"
  definition = var.definition

  depends_on = [
    aws_iam_policy.step_function_iam_policy,
  ]
}

resource "aws_cloudwatch_log_group" "state_machines" {
  name_prefix = "/aws/vendedlogs/states/${local.workspace_prefix}-state-machines"
}


