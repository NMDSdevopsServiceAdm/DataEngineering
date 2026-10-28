# Lets Publication-Output start the CircleCI release-tag pipeline once a run is
# reconciled (see "Cutting the release tag" in scripts/update_run_log_runbook.md).
#
# EventBridge creates and owns the secret behind the connection, so the CircleCI
# token is set by hand after apply and never appears in Terraform or its state.

locals {
  # Off main the call is skipped unless release_trigger_branch_test is set, and then
  # it starts the dry-run workflow, so a branch can never start a real release.
  release_trigger_enabled = terraform.workspace == "main" || var.release_trigger_branch_test
  release_trigger_dry_run = terraform.workspace != "main"
  release_pipeline_branch = terraform.workspace == "main" ? "main" : terraform.workspace

  circleci_pipeline_run_url = "https://circleci.com/api/v2/project/gh/NMDSdevopsServiceAdm/DataEngineering/pipeline/run"

  release_connection_arn = try(aws_cloudwatch_event_connection.release_trigger[0].arn, "")

  # Built from the name, not the state machine's ARN, which would be a dependency cycle.
  release_trigger_state_machine_arn = "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:${local.workspace_prefix}-Publication-Output"

  # One list per statement so that each conditional is a list of one object or an
  # empty list, which Terraform can give a common type.
  release_trigger_iam_statements = concat(
    local.release_trigger_enabled ? [
      {
        Effect   = "Allow"
        Action   = "states:InvokeHTTPEndpoint"
        Resource = local.release_trigger_state_machine_arn
        Condition = {
          StringEquals = { "states:HTTPMethod" = "POST" }
          StringLike   = { "states:HTTPEndpoint" = local.circleci_pipeline_run_url }
        }
      }
    ] : [],
    local.release_trigger_enabled ? [
      {
        Effect   = "Allow"
        Action   = "events:RetrieveConnectionCredentials"
        Resource = aws_cloudwatch_event_connection.release_trigger[0].arn
      }
    ] : [],
    local.release_trigger_enabled ? [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = aws_cloudwatch_event_connection.release_trigger[0].secret_arn
      }
    ] : [],
  )
}

resource "aws_cloudwatch_event_connection" "release_trigger" {
  count = local.release_trigger_enabled ? 1 : 0

  name               = "${local.workspace_prefix}-release-tag-circleci"
  description        = "CircleCI token that starts the release-tag pipeline. The key is set by hand."
  authorization_type = "API_KEY"

  auth_parameters {
    api_key {
      key   = "Circle-Token"
      value = "set-by-hand"
    }
  }

  lifecycle {
    # The real key is set by hand, so an apply must not put the placeholder back.
    ignore_changes = [auth_parameters]

    precondition {
      condition     = var.circleci_definition_id != ""
      error_message = "Set circleci_definition_id in variables.tf to the release pipeline's definition id (CircleCI: Project Settings, Project Setup)."
    }
  }
}
