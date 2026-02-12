locals {
  workspace_prefix               = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 20)
  pascal_case_state_machine_name = replace(title(replace(var.state_machine_name, "_", " ")), " ", "")
}

resource "aws_iam_role" "scheduler" {
  name = "${local.workspace_prefix}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = ["scheduler.amazonaws.com"]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "scheduler" {
  name = "${local.workspace_prefix}-scheduler"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = ["states:StartExecution"]
        Resource = [
          var.state_machine_arn
        ]
      },
    ]
  })
}
resource "aws_iam_role_policy_attachment" "scheduler" {
  policy_arn = aws_iam_policy.scheduler.arn
  role       = aws_iam_role.scheduler.name
}

resource "aws_scheduler_schedule" "state_machine_scheduler" {
  name        = "${local.workspace_prefix}-${local.pascal_case_state_machine_name}"
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  description = var.schedule_description

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "Europe/London"

  target {
    arn      = var.state_machine_arn
    role_arn = aws_iam_role.scheduler.arn
  }
}