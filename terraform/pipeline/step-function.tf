resource "aws_sfn_state_machine" "clean_and_validate_state_machine" {
  name     = "${local.workspace_prefix}-Clean-And-Validate-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/CleanAndValidate-StepFunction.json", {
    dataset_bucket_name                                              = module.datasets_bucket.bucket_name
    dataset_bucket_uri                                               = module.datasets_bucket.bucket_uri
    pipeline_resources_bucket_uri                                    = module.pipeline_resources.bucket_uri
    clean_ascwds_workplace_job_name                                  = module.clean_ascwds_workplace_job.job_name
    clean_ascwds_worker_job_name                                     = module.clean_ascwds_worker_job.job_name
    clean_cqc_provider_data_job_name                                 = module.clean_cqc_provider_data_job.job_name
    clean_cqc_location_data_job_name                                 = module.clean_cqc_location_data_job.job_name
    clean_ons_data_job_name                                          = module.clean_ons_data_job.job_name
    reconciliation_job_name                                          = module.reconciliation_job.job_name
    ascwds_crawler_name                                              = module.ascwds_crawler.crawler_name
    cqc_crawler_name                                                 = module.cqc_crawler.crawler_name
    ons_crawler_name                                                 = module.ons_crawler.crawler_name
    trigger_coverage_state_machine_arn                               = aws_sfn_state_machine.coverage_state_machine.arn
    trigger_ind_cqc_filled_post_estimates_pipeline_state_machine_arn = aws_sfn_state_machine.ind_cqc_filled_post_estimates_pipeline_state_machine.arn
    run_silver_validation_state_machine_arn                          = aws_sfn_state_machine.silver_validation_state_machine.arn
    pipeline_failure_lambda_function_arn                             = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "ind_cqc_filled_post_estimates_pipeline_state_machine" {
  name     = "${local.workspace_prefix}-Ind-CQC-Filled-Post-Estimates-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IndCqcFilledPostEstimatePipeline-StepFunction.json", {
    dataset_bucket_name                                                     = module.datasets_bucket.bucket_name
    dataset_bucket_uri                                                      = module.datasets_bucket.bucket_uri
    pipeline_resources_bucket_uri                                           = module.pipeline_resources.bucket_uri
    merge_ind_cqc_data_job_name                                             = module.merge_ind_cqc_data_job.job_name
    validate_merged_ind_cqc_data_job_name                                   = module.validate_merged_ind_cqc_data_job.job_name
    clean_ind_cqc_filled_posts_job_name                                     = module.clean_ind_cqc_filled_posts_job.job_name
    validate_cleaned_ind_cqc_data_job_name                                  = module.validate_cleaned_ind_cqc_data_job.job_name
    impute_ind_cqc_ascwds_and_pir_job_name                                  = module.impute_ind_cqc_ascwds_and_pir_job.job_name
    validate_imputed_ind_cqc_ascwds_and_pir_data_job_name                   = module.validate_imputed_ind_cqc_ascwds_and_pir_data_job.job_name
    prepare_features_care_home_ind_cqc_job_name                             = module.prepare_features_care_home_ind_cqc_job.job_name
    validate_features_care_home_ind_cqc_data_job_name                       = module.validate_features_care_home_ind_cqc_data_job.job_name
    prepare_features_non_res_ascwds_ind_cqc_job_name                        = module.prepare_features_non_res_ascwds_ind_cqc_job.job_name
    validate_features_non_res_ascwds_with_dormancy_ind_cqc_data_job_name    = module.validate_features_non_res_ascwds_with_dormancy_ind_cqc_data_job.job_name
    validate_features_non_res_ascwds_without_dormancy_ind_cqc_data_job_name = module.validate_features_non_res_ascwds_without_dormancy_ind_cqc_data_job.job_name
    estimate_ind_cqc_filled_posts_job_name                                  = module.estimate_ind_cqc_filled_posts_job.job_name
    validate_estimated_ind_cqc_filled_posts_data_job_name                   = module.validate_estimated_ind_cqc_filled_posts_data_job.job_name
    estimate_ind_cqc_filled_posts_by_job_role_job_name                      = module.estimate_ind_cqc_filled_posts_by_job_role_job.job_name
    validate_estimated_ind_cqc_filled_posts_by_job_role_data_job_name       = module.validate_estimated_ind_cqc_filled_posts_by_job_role_data_job.job_name
    diagnostics_on_known_filled_posts_job_name                              = module.diagnostics_on_known_filled_posts_job.job_name
    archive_filled_posts_estimates_job_name                                 = module.archive_filled_posts_estimates_job.job_name
    ind_cqc_filled_posts_crawler_name                                       = module.ind_cqc_filled_posts_crawler.crawler_name
    data_validation_reports_crawler_name                                    = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn                                    = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "bulk_download_cqc_api_state_machine" {
  name     = "${local.workspace_prefix}-Bulk-Download-CQC-API-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/BulkDownloadCQCAPIPipeline-StepFunction.json", {
    dataset_bucket_uri                           = module.datasets_bucket.bucket_uri
    bulk_cqc_locations_download_job_name         = module.bulk_cqc_locations_download_job.job_name
    bulk_cqc_providers_download_job_name         = module.bulk_cqc_providers_download_job.job_name
    flatten_cqc_ratings_job_name                 = module.flatten_cqc_ratings_job.job_name
    cqc_crawler_name                             = module.cqc_crawler.crawler_name
    sfc_crawler_name                             = module.sfc_crawler.crawler_name
    run_bronze_validation_state_machine_arn      = aws_sfn_state_machine.bronze_validation_state_machine.arn
    trigger_clean_and_validate_state_machine_arn = aws_sfn_state_machine.clean_and_validate_state_machine.arn
    pipeline_failure_lambda_function_arn         = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "direct_payments_state_machine" {
  name     = "${local.workspace_prefix}-DirectPaymentRecipientsPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/DirectPaymentRecipientsPipeline-StepFunction.json", {
    prepare_dpr_external_job_name                 = module.prepare_dpr_external_data_job.job_name
    prepare_dpr_survey_job_name                   = module.prepare_dpr_survey_data_job.job_name
    merge_dpr_data_job_name                       = module.merge_dpr_data_job.job_name
    estimate_direct_payments_job_name             = module.estimate_direct_payments_job.job_name
    split_pa_filled_posts_into_icb_areas_job_name = module.split_pa_filled_posts_into_icb_areas_job.job_name
    data_engineering_crawler_name                 = module.data_engineering_crawler.crawler_name
    dataset_bucket_uri                            = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn                 = aws_sfn_state_machine.run_crawler.arn
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

resource "aws_sfn_state_machine" "historic_direct_payments_state_machine" {
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

resource "aws_sfn_state_machine" "ingest_and_clean_capacity_tracker_data_state_machine" {
  name     = "${local.workspace_prefix}-IngestAndCleanCapacityTrackerDataPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestAndCleanCapacityTrackerDataPipeline-StepFunction.json", {
    ingest_capacity_tracker_data_job_name                    = module.ingest_capacity_tracker_data_job.job_name
    clean_capacity_tracker_care_home_job_name                = module.clean_capacity_tracker_care_home_job.job_name
    clean_capacity_tracker_non_res_job_name                  = module.clean_capacity_tracker_non_res_job.job_name
    diagnostics_on_capacity_tracker_job_name                 = module.diagnostics_on_capacity_tracker_job.job_name
    capacity_tracker_crawler_name                            = module.capacity_tracker_crawler.crawler_name
    ind_cqc_filled_posts_crawler_name                        = module.ind_cqc_filled_posts_crawler.crawler_name
    dataset_bucket_uri                                       = module.datasets_bucket.bucket_uri
    run_capacity_tracker_silver_validation_state_machine_arn = aws_sfn_state_machine.capacity_tracker_silver_validation_state_machine.arn
    run_crawler_state_machine_arn                            = aws_sfn_state_machine.run_crawler.arn
    pipeline_failure_lambda_function_arn                     = aws_lambda_function.error_notification_lambda.arn
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
    pipeline_failure_lambda_function_arn = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "ingest_cqc_pir_state_machine" {
  name     = "${local.workspace_prefix}-IngestCqcPir"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestCqcPir-StepFunction.json", {
    ingest_cqc_pir_job_name              = module.ingest_cqc_pir_data_job.job_name
    validate_pir_raw_data_job_name       = module.validate_pir_raw_data_job.job_name
    clean_cqc_pir_data_job_name          = module.clean_cqc_pir_data_job.job_name
    validate_pir_cleaned_data_job_name   = module.validate_pir_cleaned_data_job.job_name
    cqc_crawler_name                     = module.cqc_crawler.crawler_name
    data_validation_reports_crawler_name = module.data_validation_reports_crawler.crawler_name
    dataset_bucket_uri                   = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn        = aws_sfn_state_machine.run_crawler.arn
    pipeline_failure_lambda_function_arn = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "ingest_ct_care_home_state_machine" {
  name     = "${local.workspace_prefix}-Ingest-Capacity-Tracker-Care-Home"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestCapacityTrackerCareHome-StepFunction.json", {
    ingest_ct_care_home_job_name                = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_care_home_data_job_name            = module.clean_capacity_tracker_care_home_job.job_name
    validate_ct_care_home_cleaned_data_job_name = module.validate_cleaned_capacity_tracker_care_home_data_job.job_name
    ct_crawler_name                             = module.capacity_tracker_crawler.crawler_name
    data_validation_reports_crawler_name        = module.data_validation_reports_crawler.crawler_name
    dataset_bucket_uri                          = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn               = aws_sfn_state_machine.run_crawler.arn
    pipeline_failure_lambda_function_arn        = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "ingest_ct_non_res_state_machine" {
  name     = "${local.workspace_prefix}-Ingest-Capacity-Tracker-Non-Res"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestCapacityTrackerNonRes-StepFunction.json", {
    ingest_ct_non_res_job_name                = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_non_res_data_job_name            = module.clean_capacity_tracker_non_res_job.job_name
    validate_ct_non_res_cleaned_data_job_name = module.validate_cleaned_capacity_tracker_non_res_data_job.job_name
    ct_crawler_name                           = module.capacity_tracker_crawler.crawler_name
    data_validation_reports_crawler_name      = module.data_validation_reports_crawler.crawler_name
    dataset_bucket_uri                        = module.datasets_bucket.bucket_uri
    run_crawler_state_machine_arn             = aws_sfn_state_machine.run_crawler.arn
    pipeline_failure_lambda_function_arn      = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "ingest_ons_pd_state_machine" {
  name     = "${local.workspace_prefix}-IngestAndCleanONS"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/IngestAndCleanONS-StepFunction.json", {
    ingest_ons_data_job_name                          = module.ingest_ons_data_job.job_name
    validate_postcode_directory_raw_data_job_name     = module.validate_postcode_directory_raw_data_job.job_name
    clean_ons_data_job_name                           = module.clean_ons_data_job.job_name
    validate_postcode_directory_cleaned_data_job_name = module.validate_postcode_directory_cleaned_data_job.job_name
    ons_crawler_name                                  = module.ons_crawler.crawler_name
    data_validation_reports_crawler_name              = module.data_validation_reports_crawler.crawler_name
    dataset_bucket_uri                                = module.datasets_bucket.bucket_uri
    pipeline_failure_lambda_function_arn              = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "bronze_validation_state_machine" {
  name     = "${local.workspace_prefix}-Bronze-Validation-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/BronzeValidationPipeline-StepFunction.json", {
    dataset_bucket_uri                          = module.datasets_bucket.bucket_uri
    validate_ascwds_worker_raw_data_job_name    = module.validate_ascwds_worker_raw_data_job.job_name
    validate_ascwds_workplace_raw_data_job_name = module.validate_ascwds_workplace_raw_data_job.job_name
    validate_locations_api_raw_data_job_name    = module.validate_locations_api_raw_data_job.job_name
    validate_providers_api_raw_data_job_name    = module.validate_providers_api_raw_data_job.job_name
    data_validation_reports_crawler_name        = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn        = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "silver_validation_state_machine" {
  name     = "${local.workspace_prefix}-Silver-Validation-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/SilverValidationPipeline-StepFunction.json", {
    dataset_bucket_uri                              = module.datasets_bucket.bucket_uri
    validate_ascwds_worker_cleaned_data_job_name    = module.validate_ascwds_worker_cleaned_data_job.job_name
    validate_ascwds_workplace_cleaned_data_job_name = module.validate_ascwds_workplace_cleaned_data_job.job_name
    validate_locations_api_cleaned_data_job_name    = module.validate_locations_api_cleaned_data_job.job_name
    validate_providers_api_cleaned_data_job_name    = module.validate_providers_api_cleaned_data_job.job_name
    data_validation_reports_crawler_name            = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn            = aws_lambda_function.error_notification_lambda.arn
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

resource "aws_sfn_state_machine" "coverage_state_machine" {
  name     = "${local.workspace_prefix}-Coverage-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/CoveragePipeline-StepFunction.json", {
    dataset_bucket_uri                    = module.datasets_bucket.bucket_uri
    merge_coverage_data_job_name          = module.merge_coverage_data_job.job_name
    validate_merge_coverage_data_job_name = module.validate_merge_coverage_data_job.job_name
    sfc_crawler_name                      = module.sfc_crawler.crawler_name
    data_validation_reports_crawler_name  = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn  = aws_lambda_function.error_notification_lambda.arn
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
