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

resource "aws_cloudwatch_event_rule" "cqc_pir_csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-cqc-pir-csv-added"
  description = "Captures when a new CQC PIR CSV is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sfc-data-engineering-raw"]
    },
    "object": {
      "key": [ {"prefix": "domain=CQC/dataset=pir" }  ]
    }
  }
}
EOF
}

resource "aws_cloudwatch_event_rule" "ons_pd_csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-ons-pd-csv-added"
  description = "Captures when a new ONS Postcode Directory CSV is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sfc-data-engineering-raw"]
    },
    "object": {
      "key": [ {"prefix": "domain=ONS/dataset=postcode_directory" }  ]
    }
  }
}
EOF
}

resource "aws_cloudwatch_event_rule" "ct_care_home_csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-ct-care_home-csv-added"
  description = "Captures when a new Capacity Tracker care home CSV is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sfc-data-engineering-raw"]
    },
    "object": {
      "key": [ {"prefix": "domain=capacity_tracker/dataset=capacity_tracker_care_home" }  ]
    }
  }
}
EOF
}

resource "aws_cloudwatch_event_rule" "ct_non_res_csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-ct-non_res-csv-added"
  description = "Captures when a new Capacity Tracker non residential CSV is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = <<EOF
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["sfc-data-engineering-raw"]
    },
    "object": {
      "key": [ {"prefix": "domain=capacity_tracker/dataset=capacity_tracker_non_res" }  ]
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
        Effect = "Allow"
        Action = ["states:StartExecution"]
        Resource = [
          aws_sfn_state_machine.ingest_ascwds_state_machine.arn,
          aws_sfn_state_machine.ingest_cqc_pir_state_machine.arn,
          aws_sfn_state_machine.ingest_ons_pd_state_machine.arn
        ]
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

resource "aws_cloudwatch_event_target" "trigger_ingest_cqc_pir_state_machine" {
  rule      = aws_cloudwatch_event_rule.cqc_pir_csv_added.name
  target_id = "${local.workspace_prefix}-StartIngestCqcPirStateMachine"
  arn       = aws_sfn_state_machine.ingest_cqc_pir_state_machine.arn
  role_arn  = aws_iam_role.start_state_machines.arn

  input_transformer {
    input_paths = {
      bucket_name = "$.detail.bucket.name",
      key         = "$.detail.object.key",
    }
    input_template = <<EOF
    {
        "jobs": {
            "ingest_cqc_pir_data" : {
                "source": "s3://<bucket_name>/<key>"
            }
        }
    }
    EOF
  }
}

resource "aws_cloudwatch_event_target" "trigger_ingest_ons_pd_state_machine" {
  rule      = aws_cloudwatch_event_rule.ons_pd_csv_added.name
  target_id = "${local.workspace_prefix}-StartIngestONSStateMachine"
  arn       = aws_sfn_state_machine.ingest_ons_pd_state_machine.arn
  role_arn  = aws_iam_role.start_state_machines.arn

  input_transformer {
    input_paths = {
      bucket_name = "$.detail.bucket.name",
      key         = "$.detail.object.key",
    }
    input_template = <<EOF
    {
        "jobs": {
            "ingest_ons_data" : {
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
        Resource = [aws_sfn_state_machine.bulk_download_cqc_api_state_machine.arn]
      },
    ]
  })
}

resource "aws_scheduler_schedule" "bulk_download_cqc_api_schedule" {
  name        = "${local.workspace_prefix}-CqcApiSchedule"
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  description = "Regular scheduling of the CQC API bulk download pipeline on the first, eighth, fifteenth and twenty third of each month."

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 01 01,08,15,23 * ? *)"
  schedule_expression_timezone = "Europe/London"

  target {
    arn      = aws_sfn_state_machine.bulk_download_cqc_api_state_machine.arn
    role_arn = aws_iam_role.scheduler.arn
  }
}
