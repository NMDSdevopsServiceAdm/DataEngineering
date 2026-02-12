locals {
  workspace_prefix                    = substr(lower(replace(terraform.workspace, "/[^a-zA-Z0-9]+/", "-")), 0, 20)
  pascal_case_dataset_name            = replace(title(replace(var.dataset_name, "_", " ")), " ", "")
  shortened_start_state_machines_name = substr("start-${var.state_machine_name}-${var.dataset_name}", 0, 50)
}

resource "aws_cloudwatch_event_rule" "csv_added" {
  state       = terraform.workspace == "main" ? "ENABLED" : "DISABLED"
  name        = "${local.workspace_prefix}-${var.dataset_name}-csv-added"
  description = "Captures when a new dataset is uploaded to sfc-data-engineering-raw bucket"

  event_pattern = jsonencode(
    {
      source      = ["aws.s3"]
      detail-type = ["Object Created"]
      detail = {
        bucket = {
          name = ["sfc-data-engineering-raw"]
        },
        object = {
          key = [{ "prefix" : "domain=${var.domain_name}/dataset=${var.dataset_name}" }]
        }
  } })
}


resource "aws_iam_policy" "start_state_machines" {
  name = "${local.workspace_prefix}-${local.shortened_start_state_machines_name}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["states:StartExecution"]
        Resource = [
          var.state_machine_arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role" "start_state_machines" {
  name = "${local.workspace_prefix}-${local.shortened_start_state_machines_name}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["sts:AssumeRole"]
        Principal = {
          Service = "events.amazonaws.com"
        },
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "start_state_machines" {
  role       = aws_iam_role.start_state_machines.name
  policy_arn = aws_iam_policy.start_state_machines.arn
}

resource "aws_cloudwatch_event_target" "trigger_ingest_state_machine" {
  rule      = aws_cloudwatch_event_rule.csv_added.name
  target_id = "${local.workspace_prefix}-StartIngest${local.pascal_case_dataset_name}StateMachine"
  arn       = var.state_machine_arn
  role_arn  = aws_iam_role.start_state_machines.arn

  input_transformer {
    input_paths = {
      bucket_name = "$.detail.bucket.name",
      key         = "$.detail.object.key",
    }

    input_template = jsonencode({
      jobs = {
        "var.glue_job_name" = {
          source = "s3://<bucket_name>/<key>"
        }
      }
    })
  }
}
