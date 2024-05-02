resource "aws_cloudwatch_event_rule" "ascwds_csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-ascwds-csv-added"
  description = "Captures when a new ASC WDS worker or workspace CSV is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sfc-data-engineering-raw"]
    },
    "object": {
      "key": [ {"prefix": "domain=ASCWDS/dataset=worker" }, {"prefix": "domain=ASCWDS/dataset=workplace" }  ]
    }
  }
}
EOF
}

resource "aws_iam_policy" "start_state_machines" {
  name = "${local.workspace_prefix}-start-state-machines"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.ingest_ascwds_state_machine.arn]
      }
    ]
  })
}

resource "aws_iam_role" "start_state_machines" {
  name = "${local.workspace_prefix}-start-state-machines"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Effect": "Allow"
    }
  ]
}
EOF 
}

resource "aws_iam_role_policy_attachment" "start_state_machines" {
  role       = aws_iam_role.start_state_machines.name
  policy_arn = aws_iam_policy.start_state_machines.arn
}

resource "aws_cloudwatch_event_target" "trigger_ingest_ascwds_state_machine" {
  rule      = aws_cloudwatch_event_rule.ascwds_csv_added.name
  target_id = "${local.workspace_prefix}-StartIngestASCWDSStateMachine"
  arn       = aws_sfn_state_machine.ingest_ascwds_state_machine.arn
  role_arn  = aws_iam_role.start_state_machines.arn

  input_transformer {
    input_paths = {
      bucket_name = "$.detail.bucket.name",
      key         = "$.detail.object.key",
    }
    input_template = <<EOF
    {
        "jobs": {
            "ingest_ascwds_dataset" : {
                "source": "s3://<bucket_name>/<key>"
            }
        }
    }
    EOF
  }
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

resource "aws_iam_role_policy_attachment" "scheduler" {
  policy_arn = aws_iam_policy.scheduler.arn
  role       = aws_iam_role.scheduler.name
}

resource "aws_iam_policy" "scheduler" {
  name = "${local.workspace_prefix}-scheduler"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.bulk-download-cqc-api-state-machine.arn]
      },
    ]
  })
}

resource "aws_scheduler_schedule" "bulk_download_cqc_api_schedule" {
  name        = "${local.workspace_prefix}-CqcApiSchedule"
  state       = terraform.workspace == "add-timezone-to-cron" ? "ENABLED" : "DISABLED"
  description = "Regular scheduling of the CQC API bulk download pipeline on the first, eighth, fifteenth and twenty third of each month."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(00,15,30,45 15,16 01,03,08,15,23 * ? *)"
  schedule_expression_timezone = "GMT"

  target {
    arn      = aws_sfn_state_machine.bulk-download-cqc-api-state-machine.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}