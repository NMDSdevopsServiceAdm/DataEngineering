# Maps StepFunction files in step-functions/dynamic using filenames as keys
locals {
  step_functions = tomap({
    for fn in fileset("step-functions/dynamic", "*.json") :
    substr(fn, 0, length(fn) - 5) => "step-functions/dynamic/${fn}"
  })
}

# Created explicitly as required by dynamic step functions
resource "aws_sfn_state_machine" "run_crawler" {
  name       = "${local.workspace_prefix}-Run-Crawler"
  role_arn   = aws_iam_role.step_function_iam_role.arn
  type       = "STANDARD"
  definition = templatefile("step-functions/Run-Crawler.json", {})

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy
  ]
}

# Created explicitly as depends on dynamic step functions
resource "aws_sfn_state_machine" "workforce_intelligence_state_machine" {
  name     = "${local.workspace_prefix}-Workforce-Intelligence-Pipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/Workforce-Intelligence-Pipeline.json", {
    dataset_bucket_uri                              = module.datasets_bucket.bucket_uri
    dataset_bucket_name                             = module.datasets_bucket.bucket_name
    data_validation_reports_crawler_name            = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn            = aws_lambda_function.error_notification_lambda.arn
    transform_ascwds_state_machine_arn              = aws_sfn_state_machine.sf_pipelines["Transform-ASCWDS-Data"].arn
    transform_cqc_data_state_machine_arn            = aws_sfn_state_machine.sf_pipelines["Transform-CQC-Data"].arn
    trigger_ind_cqc_pipeline_state_machine_arn      = aws_sfn_state_machine.sf_pipelines["Ind-CQC-Filled-Post-Estimates"].arn
    trigger_sfc_internal_pipeline_state_machine_arn = aws_sfn_state_machine.sf_pipelines["SfC-Internal"].arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy,
    module.datasets_bucket,
    aws_sfn_state_machine.sf_pipelines,
  ]
}

# Created explicitly as depends on dynamic step functions
resource "aws_sfn_state_machine" "cqc_and_ascwds_orchestrator_state_machine" {
  name     = "${local.workspace_prefix}-CQC-And-ASCWDS-Orchestrator"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile("step-functions/CQC-And-ASCWDS-Orchestrator.json", {
    dataset_bucket_uri                               = module.datasets_bucket.bucket_uri
    dataset_bucket_name                              = module.datasets_bucket.bucket_name
    ingest_cqc_api_state_machine_arn                 = aws_sfn_state_machine.sf_pipelines["Ingest-CQC-API-Delta"].arn
    trigger_workforce_intelligence_state_machine_arn = aws_sfn_state_machine.workforce_intelligence_state_machine.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.state_machines.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  depends_on = [
    aws_iam_policy.step_function_iam_policy,
    module.datasets_bucket,
    aws_sfn_state_machine.sf_pipelines,
    aws_sfn_state_machine.workforce_intelligence_state_machine
  ]
}

resource "aws_sfn_state_machine" "sf_pipelines" {
  for_each = local.step_functions
  name     = "${local.workspace_prefix}-${each.key}"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"
  definition = templatefile(each.value, {
    # s3
    dataset_bucket_uri            = module.datasets_bucket.bucket_uri
    dataset_bucket_name           = module.datasets_bucket.bucket_name
    pipeline_resources_bucket_uri = module.pipeline_resources.bucket_uri

    # lambdas
    pipeline_failure_lambda_function_arn = aws_lambda_function.error_notification_lambda.arn
    create_snapshot_lambda_lambda_arn    = aws_lambda_function.create_snapshot_lambda.arn

    # step-functions - cannot include any from this for_each as circular dependency
    # if needed, create explicitly outside of this resource
    run_crawler_state_machine_arn = aws_sfn_state_machine.run_crawler.arn

    # jobs
    validate_ascwds_workplace_raw_data_job_name                             = module.validate_ascwds_workplace_raw_data_job.job_name
    validate_ascwds_worker_raw_data_job_name                                = module.validate_ascwds_worker_raw_data_job.job_name
    clean_ascwds_workplace_job_name                                         = module.clean_ascwds_workplace_job.job_name
    clean_ascwds_worker_job_name                                            = module.clean_ascwds_worker_job.job_name
    validate_ascwds_workplace_cleaned_data_job_name                         = module.validate_ascwds_workplace_cleaned_data_job.job_name
    validate_ascwds_worker_cleaned_data_job_name                            = module.validate_ascwds_worker_cleaned_data_job.job_name
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
    diagnostics_on_capacity_tracker_job_name                                = module.diagnostics_on_capacity_tracker_job.job_name
    archive_filled_posts_estimates_job_name                                 = module.archive_filled_posts_estimates_job.job_name
    validate_providers_api_raw_delta_data_job_name                          = module.validate_providers_api_raw_delta_data_job.job_name
    clean_cqc_provider_data_job_name                                        = module.clean_cqc_provider_data_job.job_name
    clean_cqc_location_data_job_name                                        = module.delta_clean_cqc_location_data_job.job_name
    validate_providers_api_cleaned_data_job_name                            = module.validate_providers_api_cleaned_data_job.job_name
    prepare_dpr_external_job_name                                           = module.prepare_dpr_external_data_job.job_name
    prepare_dpr_survey_job_name                                             = module.prepare_dpr_survey_data_job.job_name
    merge_dpr_data_job_name                                                 = module.merge_dpr_data_job.job_name
    estimate_direct_payments_job_name                                       = module.estimate_direct_payments_job.job_name
    split_pa_filled_posts_into_icb_areas_job_name                           = module.split_pa_filled_posts_into_icb_areas_job.job_name
    ingest_ascwds_job_name                                                  = module.ingest_ascwds_dataset_job.job_name
    ingest_cqc_pir_job_name                                                 = module.ingest_cqc_pir_data_job.job_name
    validate_pir_raw_data_job_name                                          = module.validate_pir_raw_data_job.job_name
    clean_cqc_pir_data_job_name                                             = module.clean_cqc_pir_data_job.job_name
    validate_pir_cleaned_data_job_name                                      = module.validate_pir_cleaned_data_job.job_name
    ingest_ct_care_home_job_name                                            = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_care_home_data_job_name                                        = module.clean_capacity_tracker_care_home_job.job_name
    validate_ct_care_home_cleaned_data_job_name                             = module.validate_cleaned_capacity_tracker_care_home_data_job.job_name
    ingest_ct_non_res_job_name                                              = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_non_res_data_job_name                                          = module.clean_capacity_tracker_non_res_job.job_name
    validate_ct_non_res_cleaned_data_job_name                               = module.validate_cleaned_capacity_tracker_non_res_data_job.job_name
    ingest_ons_data_job_name                                                = module.ingest_ons_data_job.job_name
    validate_postcode_directory_raw_data_job_name                           = module.validate_postcode_directory_raw_data_job.job_name
    clean_ons_data_job_name                                                 = module.clean_ons_data_job.job_name
    validate_postcode_directory_cleaned_data_job_name                       = module.validate_postcode_directory_cleaned_data_job.job_name
    flatten_cqc_ratings_job_name                                            = module.flatten_cqc_ratings_job.job_name
    merge_coverage_data_job_name                                            = module.merge_coverage_data_job.job_name
    validate_merge_coverage_data_job_name                                   = module.validate_merge_coverage_data_job.job_name
    reconciliation_job_name                                                 = module.reconciliation_job.job_name

    # crawlers
    data_validation_reports_crawler_name = module.data_validation_reports_crawler.crawler_name
    ascwds_crawler_name                  = module.ascwds_crawler.crawler_name
    ind_cqc_filled_posts_crawler_name    = module.ind_cqc_filled_posts_crawler.crawler_name
    cqc_crawler_name                     = module.cqc_crawler.crawler_name
    cqc_crawler_delta_name               = module.cqc_crawler_delta.crawler_name # TODO: remove and point back to main crawler
    dpr_crawler_name                     = module.dpr_crawler.crawler_name
    data_engineering_ascwds_crawler_name = module.ascwds_crawler.crawler_name
    ons_crawler_name                     = module.ons_crawler.crawler_name
    sfc_crawler_name                     = module.sfc_crawler.crawler_name
    ct_crawler_name                      = module.capacity_tracker_crawler.crawler_name

    # parameter store
    last_providers_run_param_name = aws_ssm_parameter.providers_last_run.name
    last_locations_run_param_name = aws_ssm_parameter.locations_last_run.name

    # ecs
    polars_cluster_arn = aws_ecs_cluster.polars_cluster.arn
    model_cluster_arn  = aws_ecs_cluster.polars_cluster.arn

    # ecs tasks
    cqc_api_task_arn         = module.cqc-api.task_arn
    independent_cqc_task_arn = module._03_independent_cqc.task_arn
    preprocess_task_arn      = module.model_preprocess.task_arn
    retrain_task_arn         = module.model_retrain.task_arn
    prediction_task_arn      = module.model_predict.task_arn

    # ecs task subnets
    cqc_api_subnet_ids          = jsonencode(module.cqc-api.subnet_ids)
    independent_cqc_subnet_ids  = jsonencode(module._03_independent_cqc.subnet_ids)
    model_subnet_ids            = jsonencode(module.model_preprocess.subnet_ids)
    model_preprocess_subnet_ids = jsonencode(module.model_preprocess.subnet_ids)

    # ecs task security groups
    cqc_api_security_group_id          = module.cqc-api.security_group_id
    independent_cqc_security_group_id  = module._03_independent_cqc.security_group_id
    model_security_group_id            = module.model_preprocess.security_group_id
    model_preprocess_security_group_id = module.model_preprocess.security_group_id

    # models
    non_res_pir_model_name = "non_res_pir"
    care_home_model_name   = "care_home_filled_posts_prediction"
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
          "states:StartExecution",
          "states:ListExecutions"
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
        "Resource" : [
          "${aws_lambda_function.error_notification_lambda.arn}*",
          "${aws_lambda_function.create_snapshot_lambda.arn}*",
          "${aws_lambda_function.check_datasets_equal.arn}*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:GetJobRuns"
        ],
        "Resource" : [
          module.delta_cqc_locations_download_job.job_arn,
          module.delta_cqc_providers_download_job.job_arn
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:GetObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ],
        "Resource" : [
          "${module.datasets_bucket.bucket_arn}/*",
          module.datasets_bucket.bucket_arn
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ssm:PutParameter",
          "ssm:GetParameter",
        ],
        "Resource" : [
          aws_ssm_parameter.providers_last_run.arn,
          aws_ssm_parameter.locations_last_run.arn
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "ecs:RunTask"
        ],
        "Resource" : [
          module.cqc-api.task_arn,
          module._03_independent_cqc.task_arn,
          aws_ecs_cluster.polars_cluster.arn,
          aws_ecs_cluster.polars_cluster.arn,
          module.model_preprocess.task_arn,
          module.model_retrain.task_arn,
          module.model_predict.task_arn
        ]
      },
      {
        Effect = "Allow",
        Action = "iam:PassRole",
        Resource = [
          module.cqc-api.task_exc_role_arn,
          module.cqc-api.task_role_arn,
          module.model_retrain.task_exc_role_arn,
          module.model_retrain.task_role_arn,
          module.model_preprocess.task_role_arn,
          module.model_preprocess.task_exc_role_arn,
          module.model_predict.task_exc_role_arn,
          module.model_predict.task_role_arn,
          module._03_independent_cqc.task_exc_role_arn,
          module.cqc-api.task_role_arn,
          module._03_independent_cqc.task_role_arn
        ],
        Condition = {
          StringLike = {
            "iam:PassedToService" = [
              "ecs-tasks.amazonaws.com",
              "events.amazonaws.com"
            ]
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}
