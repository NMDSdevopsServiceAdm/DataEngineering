resource "aws_glue_catalog_database" "glue_catalog_database" {
  name        = "${local.workspace_prefix}-${var.glue_database_name}"
  description = "Database for all datasets belonging to the ${local.workspace_prefix} environment."
}

module "ingest_capacity_tracker_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/capacity_tracker/jobs"
  script_name     = "ingest_capacity_tracker_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--source"      = ""
    "--destination" = ""
  }
}

module "clean_cqc_pir_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/cqc_pir/jobs"
  script_name     = "clean_cqc_pir_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--cqc_pir_source"              = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir/"
    "--cleaned_cqc_pir_destination" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir_cleaned/"
  }
}

module "clean_ascwds_worker_job" {
  source            = "../modules/glue-job"
  script_dir        = "projects/_01_ingest/ascwds/jobs"
  script_name       = "clean_ascwds_worker_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "5.0"

  job_parameters = {
    "--ascwds_worker_source"            = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker/"
    "--ascwds_workplace_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace_cleaned/"
    "--ascwds_worker_destination"       = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker_cleaned/"
  }
}

module "clean_capacity_tracker_care_home_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/capacity_tracker/jobs"
  script_name     = "clean_capacity_tracker_care_home_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--capacity_tracker_care_home_source"              = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home/"
    "--cleaned_capacity_tracker_care_home_destination" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home_cleaned/"
  }
}

module "clean_capacity_tracker_non_res_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/capacity_tracker/jobs"
  script_name     = "clean_capacity_tracker_non_res_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--capacity_tracker_non_res_source"              = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential/"
    "--cleaned_capacity_tracker_non_res_destination" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential_cleaned/"
  }
}


module "ingest_dpr_external_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/direct_payment_recipients/jobs"
  script_name     = "ingest_dpr_external_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--external_data_source"      = ""
    "--external_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2026.02/"
  }
}

module "ingest_dpr_survey_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/direct_payment_recipients/jobs"
  script_name     = "ingest_dpr_survey_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--survey_data_source"      = ""
    "--survey_data_destination" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2026.02/"
  }
}

module "prepare_dpr_external_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_04_direct_payment_recipients/jobs"
  script_name     = "prepare_dpr_external_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"
  job_parameters = {
    "--direct_payments_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external/version=2026.02/"
    "--destination"            = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external_prepared/version=2026.02/"
  }
}

module "prepare_dpr_survey_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_04_direct_payment_recipients/jobs"
  script_name     = "prepare_dpr_survey_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"
  job_parameters = {
    "--survey_data_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey/version=2026.02/"
    "--destination"        = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey_prepared/version=2026.02/"
  }
}

module "merge_dpr_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_04_direct_payment_recipients/jobs"
  script_name     = "merge_dpr_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"
  job_parameters = {
    "--direct_payments_external_data_source" = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_external_prepared/version=2026.02/"
    "--direct_payments_survey_data_source"   = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_survey_prepared/version=2026.02/"
    "--destination"                          = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_merged/version=2026.02/"
  }
}

module "split_pa_filled_posts_into_icb_areas_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_04_direct_payment_recipients/jobs"
  script_name     = "split_pa_filled_posts_into_icb_areas.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"
  job_parameters = {
    "--postcode_directory_source" = "${module.datasets_bucket.bucket_uri}/domain=ONS/dataset=postcode_directory_cleaned/"
    "--pa_filled_posts_souce"     = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_estimates/version=2026.02/"
    "--destination"               = "${module.datasets_bucket.bucket_uri}/domain=DPR/dataset=direct_payments_estimates_by_icb/version=2026.02/"
  }
}

module "flatten_cqc_ratings_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_02_sfc_internal/cqc_ratings/jobs"
  script_name     = "flatten_cqc_ratings.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_full_snapshot_source"       = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_locations_04_latest_snapshot/"
    "--cqc_locations_api_delta_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_locations_01_delta_api/version=3.1.7/"
    "--ascwds_workplace_source"        = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=workplace/"
    "--cqc_ratings_destination"        = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_data_requests/"
    "--benchmark_ratings_destination"  = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_benchmarks/version=2.0.0/"
  }
}

module "reconciliation_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_02_sfc_internal/reconciliation/jobs"
  script_name     = "reconciliation.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--cqc_locations_snapshot_source"              = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_locations_04_latest_snapshot/"
    "--ascwds_workplace_source"                    = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=ascwds_for_sfc_internal/"
    "--reconciliation_single_and_subs_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_reconciliation_singles_and_subs"
    "--reconciliation_parents_destination"         = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_reconciliation_parents"
  }
}


module "merge_coverage_data_job" {
  source            = "../modules/glue-job"
  script_dir        = "projects/_02_sfc_internal/cqc_coverage/jobs"
  script_name       = "merge_coverage_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "5.0"
  worker_type       = "G.2X"
  number_of_workers = 5

  job_parameters = {
    "--cleaned_cqc_location_source"  = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_locations_04_full_cleaned_registered/"
    "--ascwds_workplace_source"      = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=ascwds_for_sfc_internal/"
    "--cqc_ratings_source"           = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_cqc_ratings_for_data_requests/"
    "--cleaned_cqc_providers_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_providers_04_full_cleaned/"
    "--merged_coverage_destination"  = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_merged_coverage_data/"
    "--reduced_coverage_destination" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_monthly_coverage_data/"
  }
}

module "validate_pir_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/cqc_pir/jobs"
  script_name     = "validate_pir_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--cleaned_cqc_pir_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=pir_cleaned/"
    "--report_destination"     = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=validation_pdq_pir_cleaned/"
  }
}

module "validate_ascwds_worker_cleaned_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/ascwds/jobs"
  script_name     = "validate_ascwds_worker_cleaned_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--cleaned_ascwds_worker_source" = "${module.datasets_bucket.bucket_uri}/domain=ASCWDS/dataset=worker_cleaned/"
    "--report_destination"           = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=validation_pdq_worker_cleaned/"
  }
}

module "validate_merge_coverage_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_02_sfc_internal/cqc_coverage/jobs"
  script_name     = "validate_merge_coverage_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--cleaned_cqc_location_source" = "${module.datasets_bucket.bucket_uri}/domain=CQC/dataset=cqc_locations_04_full_cleaned_registered/"
    "--merged_coverage_data_source" = "${module.datasets_bucket.bucket_uri}/domain=SfC/dataset=sfc_merged_coverage_data/"
    "--report_destination"          = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=validation_pdq_sfc_merged_coverage_data/"
  }
}


module "diagnostics_on_known_filled_posts_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_03_independent_cqc/_08_diagnostics/jobs"
  script_name     = "diagnostics_on_known_filled_posts.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket

  job_parameters = {
    "--estimate_filled_posts_source"    = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_06_estimated_filled_posts/"
    "--diagnostics_destination"         = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_estimated_filled_posts_diagnostics/"
    "--summary_diagnostics_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_estimated_filled_posts_diagnostics_summary/"
    "--charts_destination"              = "${module.datasets_bucket.bucket_name}"
  }
}

module "diagnostics_on_capacity_tracker_job" {
  source            = "../modules/glue-job"
  script_dir        = "projects/_03_independent_cqc/_08_diagnostics/jobs"
  script_name       = "diagnostics_on_capacity_tracker.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  worker_type       = "G.1X"
  number_of_workers = 4
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket

  job_parameters = {
    "--estimate_filled_posts_source"              = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_06_estimated_filled_posts/"
    "--care_home_diagnostics_destination"         = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_capacity_tracker_care_home_diagnostics/"
    "--care_home_summary_diagnostics_destination" = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_capacity_tracker_care_home_diagnostics_summary/"
    "--non_res_diagnostics_destination"           = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_capacity_tracker_non_residential_diagnostics/"
    "--non_res_summary_diagnostics_destination"   = "${module.datasets_bucket.bucket_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_08_capacity_tracker_non_residential_diagnostics_summary/"
  }
}

module "validate_cleaned_capacity_tracker_care_home_data_job" {
  source            = "../modules/glue-job"
  script_dir        = "projects/_01_ingest/capacity_tracker/jobs"
  script_name       = "validate_cleaned_capacity_tracker_care_home_data.py"
  glue_role         = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket   = module.pipeline_resources
  datasets_bucket   = module.datasets_bucket
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4

  job_parameters = {
    "--capacity_tracker_care_home_source"         = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home/"
    "--capacity_tracker_care_home_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_care_home_cleaned/"
    "--report_destination"                        = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=validation_pdq_capacity_tracker_care_home_cleaned_data/"
  }
}

module "validate_cleaned_capacity_tracker_non_res_data_job" {
  source          = "../modules/glue-job"
  script_dir      = "projects/_01_ingest/capacity_tracker/jobs"
  script_name     = "validate_cleaned_capacity_tracker_non_res_data.py"
  glue_role       = aws_iam_role.sfc_glue_service_iam_role
  resource_bucket = module.pipeline_resources
  datasets_bucket = module.datasets_bucket
  glue_version    = "5.0"

  job_parameters = {
    "--capacity_tracker_non_res_source"         = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential/"
    "--capacity_tracker_non_res_cleaned_source" = "${module.datasets_bucket.bucket_uri}/domain=capacity_tracker/dataset=capacity_tracker_non_residential_cleaned/"
    "--report_destination"                      = "${module.datasets_bucket.bucket_uri}/domain=data_validation_reports/dataset=validation_pdq_capacity_tracker_non_residential_cleaned_data/"
  }
}

module "ascwds_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ASCWDS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ind_cqc_filled_posts_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ind_cqc_filled_posts"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "data_validation_reports_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "data_validation_reports"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "cqc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "CQC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "sfc_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "SfC"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "ons_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "ONS"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=postcode-directory-field-lookups/**"]
}

module "dpr_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "DPR"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "capacity_tracker_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "capacity_tracker"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}

module "workforce_characteristics_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "workforce_characteristics"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
  exclusions                   = ["dataset=empstat_rates/**"]
}

module "sample_archive_data_crawler" {
  source                       = "../modules/glue-crawler"
  dataset_for_crawler          = "sample_archive_data"
  glue_role                    = aws_iam_role.sfc_glue_service_iam_role
  workspace_glue_database_name = "${local.workspace_prefix}-${var.glue_database_name}"
}
