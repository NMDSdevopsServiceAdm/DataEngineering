resource "aws_sfn_state_machine" "ethnicity-breakdown-state-machine" {
  name     = "${local.workspace_prefix}-EthnicityBreakdownPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/EthnicityBreakdownPipeline-StepFunction.json", {
    ingest_ascwds_job_name               = module.ingest_ascwds_dataset_job.job_name
    prepare_locations_job_name           = module.prepare_locations_job.job_name
    estimate_2021_jobs_job_name          = module.estimate_2021_jobs_job.job_name
    data_engineering_ascwds_crawler_name = module.ascwds_crawler.crawler_name
    data_engineering_crawler_name        = module.data_engineering_crawler.crawler_name
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

resource "aws_cloudwatch_log_group" "state_machines" {
  name_prefix = "${local.workspace_prefix}-state-machines"
}

resource "aws_sfn_state_machine" "transform_ascwds_state_machine" {
  name     = "${local.workspace_prefix}-TransformASCWDS"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/TransformASCWDS-StepFunction.json", {
    ingest_ascwds_job_name               = module.ingest_ascwds_dataset_job.job_name
    data_engineering_ascwds_crawler_name = module.ascwds_crawler.crawler_name
    dataset_bucket_name                  = module.datasets_bucket.bucket_name
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

resource "aws_iam_role" "step_function_iam_role" {
  name               = "${local.workspace_prefix}-AWSStepFunction-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_iam_policy.json
}

data "aws_iam_policy_document" "step_function_iam_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "step_function_iam_policy" {
  name        = "${local.workspace_prefix}-step_function_iam_policy"
  path        = "/"
  description = "IAM Policy for step functions"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ],
        "Resource" : [
          "*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:StartCrawler",
          "glue:StartJobRun",
          "glue:GetJobRun"
        ],
        "Resource" : "*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ],
        "Resource" : "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}
