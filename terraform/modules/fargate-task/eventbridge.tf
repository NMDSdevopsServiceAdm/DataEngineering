resource "aws_iam_policy" "scheduler_fg" {
  name = "${local.workspace_prefix}-fargate-scheduler"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = ["states:StartExecution"]
        Resource = [
          aws_sfn_state_machine.polars_task_step_function.arn,
        ]
      },
    ]
  })
}

resource "aws_iam_role" "scheduler_fg" {
  name = "${local.workspace_prefix}-fargate-scheduler"
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

resource "aws_iam_role_policy_attachment" "scheduler_fg" {
  policy_arn = aws_iam_policy.scheduler_fg.arn
  role       = aws_iam_role.scheduler_fg.name
}

resource "aws_scheduler_schedule" "polars_download_cqc_api_schedule" {
  name        = "${local.workspace_prefix}-CqcApiSchedule-Polars"
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  description = "Regular scheduling of the CQC API polars download pipeline on the first, eighth, fifteenth and twenty third of each month."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(15 01 01,08,15,23 * ? *)"
  schedule_expression_timezone = "Europe/London"

  target {
    arn      = aws_sfn_state_machine.polars_task_step_function.arn
    role_arn = aws_iam_role.scheduler_fg.arn
  }
}