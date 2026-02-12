locals {
  # Maps StepFunction files in step-functions/dynamic using filenames as keys
  step_functions = tomap({
    for fn in fileset("step-functions/dynamic", "*.json") :
    substr(fn, 0, length(fn) - 5) => "step-functions/dynamic/${fn}"
  })
}

# Created explicitly as required by dynamic step functions
module "run_crawler" {
  source              = "../modules/step-function"
  pipeline_name       = "Run-Crawler"
  dataset_bucket_name = module.datasets_bucket.bucket_name
  definition          = templatefile("step-functions/Run-Crawler.json", {})
}


# Created explicitly as depends on dynamic step functions
module "workforce_intelligence_state_machine" {
  source              = "../modules/step-function"
  pipeline_name       = "Workforce-Intelligence-Pipeline"
  dataset_bucket_name = module.datasets_bucket.bucket_name
  definition = templatefile("step-functions/Workforce-Intelligence-Pipeline.json", {
    dataset_bucket_uri                      = module.datasets_bucket.bucket_uri
    dataset_bucket_name                     = module.datasets_bucket.bucket_name
    dataset_bucket_arn                      = module.datasets_bucket.bucket_arn
    data_validation_reports_crawler_name    = module.data_validation_reports_crawler.crawler_name
    pipeline_failure_lambda_function_arn    = aws_lambda_function.error_notification_lambda.arn
    transform_ascwds_state_machine_arn      = module.sf_pipelines["Transform-ASCWDS-Data"].pipeline_arn
    transform_cqc_data_state_machine_arn    = module.sf_pipelines["Transform-CQC-Data"].pipeline_arn
    ind_cqc_pipeline_state_machine_arn      = module.sf_pipelines["Ind-CQC-Filled-Post-Estimates"].pipeline_arn
    sfc_internal_pipeline_state_machine_arn = module.sf_pipelines["SfC-Internal"].pipeline_arn
  })

}

# Created explicitly as depends on dynamic step functions
module "cqc_and_ascwds_orchestrator_state_machine" {
  source              = "../modules/step-function"
  pipeline_name       = "CQC-And-ASCWDS-Orchestrator"
  dataset_bucket_name = module.datasets_bucket.bucket_name
  definition = templatefile("step-functions/CQC-And-ASCWDS-Orchestrator.json", {
    dataset_bucket_uri                       = module.datasets_bucket.bucket_uri
    dataset_bucket_name                      = module.datasets_bucket.bucket_name
    dataset_bucket_arn                       = module.datasets_bucket.bucket_arn
    ingest_cqc_api_state_machine_arn         = module.sf_pipelines["Ingest-CQC-API-Delta"].pipeline_arn
    workforce_intelligence_state_machine_arn = module.workforce_intelligence_state_machine.pipeline_arn
  })

}

module "sf_pipelines" {
  source              = "../modules/step-function"
  for_each            = local.step_functions
  pipeline_name       = each.key
  dataset_bucket_name = module.datasets_bucket.bucket_name
  definition = templatefile(each.value, {
    # s3
    dataset_bucket_uri            = module.datasets_bucket.bucket_uri
    dataset_bucket_name           = module.datasets_bucket.bucket_name
    pipeline_resources_bucket_uri = module.pipeline_resources.bucket_uri
    dataset_bucket_arn            = module.datasets_bucket.bucket_arn

    # lambdas
    pipeline_failure_lambda_function_arn = aws_lambda_function.error_notification_lambda.arn

    # step-functions - cannot include any from this for_each as circular dependency
    # if needed, create explicitly outside of this resource
    run_crawler_state_machine_arn = module.run_crawler.pipeline_arn

    # jobs
    validate_ascwds_workplace_raw_data_job_name                          = module.validate_ascwds_workplace_raw_data_job.job_name
    validate_ascwds_worker_raw_data_job_name                             = module.validate_ascwds_worker_raw_data_job.job_name
    clean_ascwds_workplace_job_name                                      = module.clean_ascwds_workplace_job.job_name
    clean_ascwds_worker_job_name                                         = module.clean_ascwds_worker_job.job_name
    validate_ascwds_workplace_cleaned_data_job_name                      = module.validate_ascwds_workplace_cleaned_data_job.job_name
    validate_ascwds_worker_cleaned_data_job_name                         = module.validate_ascwds_worker_cleaned_data_job.job_name
    validate_merged_ind_cqc_data_job_name                                = module.validate_merged_ind_cqc_data_job.job_name
    clean_ind_cqc_filled_posts_job_name                                  = module.clean_ind_cqc_filled_posts_job.job_name
    validate_cleaned_ind_cqc_data_job_name                               = module.validate_cleaned_ind_cqc_data_job.job_name
    impute_ind_cqc_ascwds_and_pir_job_name                               = module.impute_ind_cqc_ascwds_and_pir_job.job_name
    validate_imputed_ind_cqc_ascwds_and_pir_data_job_name                = module.validate_imputed_ind_cqc_ascwds_and_pir_data_job.job_name
    prepare_features_non_res_ascwds_ind_cqc_job_name                     = module.prepare_features_non_res_ascwds_ind_cqc_job.job_name
    validate_features_non_res_ascwds_with_dormancy_ind_cqc_data_job_name = module.validate_features_non_res_ascwds_with_dormancy_ind_cqc_data_job.job_name
    estimate_ind_cqc_filled_posts_job_name                               = module.estimate_ind_cqc_filled_posts_job.job_name
    validate_estimated_ind_cqc_filled_posts_data_job_name                = module.validate_estimated_ind_cqc_filled_posts_data_job.job_name
    estimate_ind_cqc_filled_posts_by_job_role_job_name                   = module.estimate_ind_cqc_filled_posts_by_job_role_job.job_name
    validate_estimated_ind_cqc_filled_posts_by_job_role_data_job_name    = module.validate_estimated_ind_cqc_filled_posts_by_job_role_data_job.job_name
    diagnostics_on_known_filled_posts_job_name                           = module.diagnostics_on_known_filled_posts_job.job_name
    diagnostics_on_capacity_tracker_job_name                             = module.diagnostics_on_capacity_tracker_job.job_name
    archive_filled_posts_estimates_job_name                              = module.archive_filled_posts_estimates_job.job_name
    prepare_dpr_external_job_name                                        = module.prepare_dpr_external_data_job.job_name
    prepare_dpr_survey_job_name                                          = module.prepare_dpr_survey_data_job.job_name
    merge_dpr_data_job_name                                              = module.merge_dpr_data_job.job_name
    estimate_direct_payments_job_name                                    = module.estimate_direct_payments_job.job_name
    split_pa_filled_posts_into_icb_areas_job_name                        = module.split_pa_filled_posts_into_icb_areas_job.job_name
    ingest_ascwds_job_name                                               = module.ingest_ascwds_dataset_job.job_name
    ingest_cqc_pir_job_name                                              = module.ingest_cqc_pir_data_job.job_name
    validate_pir_raw_data_job_name                                       = module.validate_pir_raw_data_job.job_name
    clean_cqc_pir_data_job_name                                          = module.clean_cqc_pir_data_job.job_name
    validate_pir_cleaned_data_job_name                                   = module.validate_pir_cleaned_data_job.job_name
    ingest_ct_care_home_job_name                                         = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_care_home_data_job_name                                     = module.clean_capacity_tracker_care_home_job.job_name
    validate_ct_care_home_cleaned_data_job_name                          = module.validate_cleaned_capacity_tracker_care_home_data_job.job_name
    ingest_ct_non_res_job_name                                           = module.ingest_capacity_tracker_data_job.job_name
    clean_ct_non_res_data_job_name                                       = module.clean_capacity_tracker_non_res_job.job_name
    validate_ct_non_res_cleaned_data_job_name                            = module.validate_cleaned_capacity_tracker_non_res_data_job.job_name
    ingest_ons_data_job_name                                             = module.ingest_ons_data_job.job_name
    validate_postcode_directory_raw_data_job_name                        = module.validate_postcode_directory_raw_data_job.job_name
    clean_ons_data_job_name                                              = module.clean_ons_data_job.job_name
    validate_postcode_directory_cleaned_data_job_name                    = module.validate_postcode_directory_cleaned_data_job.job_name
    flatten_cqc_ratings_job_name                                         = module.flatten_cqc_ratings_job.job_name
    merge_coverage_data_job_name                                         = module.merge_coverage_data_job.job_name
    validate_merge_coverage_data_job_name                                = module.validate_merge_coverage_data_job.job_name
    reconciliation_job_name                                              = module.reconciliation_job.job_name

    # crawlers
    data_validation_reports_crawler_name = module.data_validation_reports_crawler.crawler_name
    ascwds_crawler_name                  = module.ascwds_crawler.crawler_name
    ind_cqc_filled_posts_crawler_name    = module.ind_cqc_filled_posts_crawler.crawler_name
    cqc_crawler_name                     = module.cqc_crawler.crawler_name
    dpr_crawler_name                     = module.dpr_crawler.crawler_name
    ons_crawler_name                     = module.ons_crawler.crawler_name
    sfc_crawler_name                     = module.sfc_crawler.crawler_name
    ct_crawler_name                      = module.capacity_tracker_crawler.crawler_name

    # parameter store
    last_providers_run_param_name = aws_ssm_parameter.providers_last_run.name
    last_locations_run_param_name = aws_ssm_parameter.locations_last_run.name

    # ecs
    polars_cluster_arn = "arn:aws:ecs:eu-west-2:${data.aws_caller_identity.current.account_id}:cluster/${local.workspace_prefix}-cluster"

    cqc_api_public_subnet_ids               = jsonencode(module.cqc-api.subnet_ids)
    independent_cqc_public_subnet_ids       = jsonencode(module._03_independent_cqc.subnet_ids)
    independent_cqc_model_public_subnet_ids = jsonencode(module._03_independent_cqc_model.subnet_ids)

    # ecs tasks
    cqc_api_task_arn               = module.cqc-api.task_arn
    independent_cqc_task_arn       = module._03_independent_cqc.task_arn
    independent_cqc_model_task_arn = module._03_independent_cqc_model.task_arn

    # ecs task security groups
    cqc_api_security_group_id               = module.cqc-api.security_group_id
    independent_cqc_security_group_id       = module._03_independent_cqc.security_group_id
    independent_cqc_model_security_group_id = module._03_independent_cqc_model.security_group_id

    # models
    preprocessor_name = "preprocess_non_res_pir"
    model_name        = "non_res_pir"
  })
}





