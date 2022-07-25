resource "aws_sfn_state_machine" "data-engineering-state-machine" {
  name     = "${local.workspace_prefix}-DataEngineeringPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/DataEngineeringPipeline-StepFunction.json", {
    prepare_locations_job_name             = module.prepare_locations_job.job_name
    prepare_workers_job_name               = module.prepare_workers_job.job_name
    estimate_jobs_job_name                 = module.estimate_job_counts_job.job_name
    locations_feature_engineering_job_name = module.locations_feature_engineering_job.job_name
    job_role_breakdown_job_name            = module.job_role_breakdown_job.job_name
    data_engineering_crawler_name          = module.data_engineering_crawler.crawler_name
    pipeline_resources_bucket_uri          = module.pipeline_resources.bucket_uri
    dataset_bucket_uri                     = module.datasets_bucket.bucket_uri
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy
  ]
}

resource "aws_sfn_state_machine" "ingest_ascwds_state_machine" {
  name     = "${local.workspace_prefix}-IngestASCWDS"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestASCWDS-StepFunction.json", {
    ingest_ascwds_job_name               = module.ingest_ascwds_dataset_job.job_name
    data_engineering_ascwds_crawler_name = module.ascwds_crawler.crawler_name
    dataset_bucket_name                  = module.datasets_bucket.bucket_name
    run_crawler_state_machine_arn        = aws_sfn_state_machine.run_crawler.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy
  ]
}

resource "aws_sfn_state_machine" "run_crawler" {
  name       = "${local.workspace_prefix}-RunCrawler"
  role_arn   = aws_iam_role.step_function_iam_role.arn
  type       = "STANDARD"
  definition = templatefile("step-functions/RunCrawler-StepFunction.json", {})

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy
  ]
}

resource "aws_cloudwatch_log_group" "state_machines" {
  name_prefix = "/aws/vendedlogs/states/${local.workspace_prefix}-state-machines"
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
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "states:StartExecution"
        ],
        "Resource" : [
          "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:stateMachine:*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "states:DescribeExecution",
          "states:StopExecution"
        ],
        "Resource" : [
          "arn:aws:states:eu-west-2:${data.aws_caller_identity.current.account_id}:execution:*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
        ],
        "Resource" : "arn:aws:events:eu-west-2:${data.aws_caller_identity.current.account_id}:rule/StepFunctions*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}
