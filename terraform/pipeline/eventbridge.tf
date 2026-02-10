module "cqc_and_ascwds_orchestrator_scheduler" {
  source               = "../modules/eventbridge-scheduler"
  state_machine_name   = "cqc_and_ascwds_orchestrator"
  state_machine_arn    = aws_sfn_state_machine.cqc_and_ascwds_orchestrator_state_machine.arn
  schedule_description = "Regular scheduling of the CQC and ASCWDS Orchestrator pipeline on the first, eighth, fifteenth and twenty third of each month."
  schedule_expression  = "cron(00 01 01,08,15,23 * ? *)"
}

module "ascwds_workplace_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "ASCWDS"
  dataset_name  = "workplace"
  pipeline_name = "Ingest-ASCWDS"
  glue_job_name = "ingest_ascwds_dataset"
}

module "ascwds_worker_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "ASCWDS"
  dataset_name  = "worker"
  pipeline_name = "Ingest-ASCWDS"
  glue_job_name = "ingest_ascwds_dataset"
}

module "cqc_pir_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "CQC"
  dataset_name  = "pir"
  pipeline_name = "Ingest-CQC-PIR"
  glue_job_name = "ingest_cqc_pir_data"
}

module "ons_pd_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "ONS"
  dataset_name  = "postcode_directory"
  pipeline_name = "Ingest-ONSPD"
  glue_job_name = "ingest_ons_data"
}

module "ct_care_home_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "capacity_tracker"
  dataset_name  = "capacity_tracker_care_home"
  pipeline_name = "Ingest-Capacity-Tracker-Care-Home"
  glue_job_name = "ingest_capacity_tracker_data"
}

module "ct_non-res_csv_added" {
  source        = "../modules/eventbridge-trigger"
  domain_name   = "capacity_tracker"
  dataset_name  = "capacity_tracker_non_res"
  pipeline_name = "Ingest-Capacity-Tracker-Non-Res"
  glue_job_name = "ingest_capacity_tracker_data"
}