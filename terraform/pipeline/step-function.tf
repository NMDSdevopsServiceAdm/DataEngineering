resource "aws_sfn_state_machine" "ind-cqc-filled-post-estimates-pipeline-state-machine" {
  name     = "${local.workspace_prefix}-Ind-CQC-Filled-Post-Estimates-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IndCqcFilledPostEstimatePipeline-StepFunction.json", {
    dataset_bucket_uri                                 = module.datasets_bucket.bucket_uri
    pipeline_resources_bucket_uri                      = module.pipeline_resources.bucket_uri
    clean_ascwds_workplace_job_name                    = module.clean_ascwds_workplace_job.job_name
    clean_ascwds_worker_job_name                       = module.clean_ascwds_worker_job.job_name
    clean_cqc_pir_data_job_name                        = module.clean_cqc_pir_data_job.job_name
    clean_cqc_provider_data_job_name                   = module.clean_cqc_provider_data_job.job_name
    clean_cqc_location_data_job_name                   = module.clean_cqc_location_data_job.job_name
    clean_ons_data_job_name                            = module.clean_ons_data_job.job_name
    merge_ind_cqc_data_job_name                        = module.merge_ind_cqc_data_job.job_name
    validate_merged_ind_cqc_data_job_name              = module.validate_merged_ind_cqc_data_job.job_name
    clean_ind_cqc_filled_posts_job_name                = module.clean_ind_cqc_filled_posts_job.job_name
    prepare_care_home_ind_cqc_features_job_name        = module.prepare_care_home_ind_cqc_features_job.job_name
    estimate_ind_cqc_filled_posts_job_name             = module.estimate_ind_cqc_filled_posts_job.job_name
    estimate_ind_cqc_filled_posts_by_job_role_job_name = module.estimate_ind_cqc_filled_posts_by_job_role_job.job_name
    prepare_non_res_ind_cqc_features_job_name          = module.prepare_non_res_ind_cqc_features_job.job_name
    ascwds_crawler_name                                = module.ascwds_crawler.crawler_name
    cqc_crawler_name                                   = module.cqc_crawler.crawler_name
    ind_cqc_filled_posts_crawler_name                  = module.ind_cqc_filled_posts_crawler.crawler_name
    ons_crawler_name                                   = module.ons_crawler.crawler_name
    data_validation_reports_crawler_name               = module.data_validation_reports_crawler.crawler_name
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy,
    module.datasets_bucket
  ]
}

resource "aws_sfn_state_machine" "direct-payments-state-machine" {
  name     = "${local.workspace_prefix}-DirectPaymentRecipientsPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/DirectPaymentRecipientsPipeline-StepFunction.json", {
    prepare_dpr_external_job_name     = module.prepare_dpr_external_data_job.job_name
    prepare_dpr_survey_job_name       = module.prepare_dpr_survey_data_job.job_name
    merge_dpr_data_job_name           = module.merge_dpr_data_job.job_name
    estimate_direct_payments_job_name = module.estimate_direct_payments_job.job_name
    data_engineering_crawler_name     = module.data_engineering_crawler.crawler_name
    dataset_bucket_uri                = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn     = aws_sfn_state_machine.run_crawler.arn
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

resource "aws_sfn_state_machine" "historic-direct-payments-state-machine" {
  name     = "${local.workspace_prefix}-HistoricDirectPaymentRecipientsPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/HistoricDirectPaymentRecipientsPipeline-StepFunction.json", {
    prepare_dpr_external_job_name     = module.prepare_dpr_external_data_job.job_name
    estimate_direct_payments_job_name = module.estimate_direct_payments_job.job_name
    data_engineering_crawler_name     = module.data_engineering_crawler.crawler_name
    dataset_bucket_uri                = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn     = aws_sfn_state_machine.run_crawler.arn
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
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "SNS:Publish"
        ],
        "Resource" : "${aws_sns_topic.pipeline_failures.arn}"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "lambda:InvokeFunction"
        ],
        "Resource" : "${aws_lambda_function.error_notification_lambda.arn}*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}
