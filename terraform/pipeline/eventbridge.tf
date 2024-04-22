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
        Resource = [aws_sfn_state_machine.ingest_ascwds_state_machine.arn, aws_sfn_state_machine.bulk-download-cqc-api-state-machine.arn]
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

resource "aws_iam_role" "scheduler_execution_role" {
  name = "${local.workspace_prefix}-scheduler_execution_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "scheduler.amazonaws.com"
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

resource "aws_scheduler_schedule" "bulk_download_cqc_api_schedule" {
  name       = "Bulk-Download-CQC-API-Pipeline-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(30 01 01,08,15,23 * ? *)"

  target {
    arn      = aws_sfn_state_machine.bulk-download-cqc-api-state-machine.arn
    role_arn = aws_iam_role.scheduler_execution_role.arn
  }
}