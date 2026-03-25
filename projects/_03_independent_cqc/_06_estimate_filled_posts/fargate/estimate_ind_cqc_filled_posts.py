from polars_utils import utils
from projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.utils import (
    enrich_with_model_predictions,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

ind_cqc_columns = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.care_home_status_count,
    IndCQC.number_of_beds,
    IndCQC.number_of_beds_banded,
    IndCQC.regulated_activities_offered,
    IndCQC.specialisms_offered,
    IndCQC.specialism_dementia,
    IndCQC.specialism_learning_disabilities,
    IndCQC.specialism_mental_health,
    IndCQC.imputed_registration_date,
    IndCQC.time_registered,
    IndCQC.related_location,
    IndCQC.dormancy,
    IndCQC.time_since_dormant,
    IndCQC.registered_manager_names,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.contemporary_cssr,
    IndCQC.contemporary_region,
    IndCQC.contemporary_sub_icb,
    IndCQC.contemporary_icb,
    IndCQC.contemporary_icb_region,
    IndCQC.ct_non_res_import_date,
    IndCQC.ct_non_res_care_workers_employed,
    IndCQC.ct_non_res_filtering_rule,
    IndCQC.ct_non_res_care_workers_employed_cleaned,
    IndCQC.ct_non_res_care_workers_employed_imputed,
    IndCQC.ct_care_home_import_date,
    IndCQC.ct_care_home_total_employed,
    IndCQC.ct_care_home_filtering_rule,
    IndCQC.ct_care_home_total_employed_cleaned,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.cqc_pir_import_date,
    IndCQC.pir_people_directly_employed_dedup,
    IndCQC.pir_filled_posts_model,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.imputed_filled_post_model,
    IndCQC.imputed_filled_posts_per_bed_ratio_model,
    IndCQC.posts_rolling_average_model,
    IndCQC.unix_time,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    bucket_name: str,
    imputed_ind_cqc_data_source: str,
    destination: str,
) -> None:
    print("Estimating independent CQC filled posts...")

    lf = utils.scan_parquet(
        source=imputed_ind_cqc_data_source,
        selected_columns=ind_cqc_columns,
    )
    print("Imputed independent CQC LazyFrame read in")

    lf = enrich_with_model_predictions(lf, bucket_name, IndCQC.care_home_model)
    lf = enrich_with_model_predictions(
        lf, bucket_name, IndCQC.non_res_with_dormancy_model
    )
    lf = enrich_with_model_predictions(
        lf, bucket_name, IndCQC.non_res_without_dormancy_model
    )

    # combine_non_res_with_and_without_dormancy_models

    # model_imputation_with_extrapolation_and_interpolation - imputed_posts_care_home_model

    # model_imputation_with_extrapolation_and_interpolation - imputed_posts_non_res_combined_model

    # model_imputation_with_extrapolation_and_interpolation - imputed_pir_filled_posts_model

    # merge_columns_in_order

    # set_min_value

    # estimate_non_res_capacity_tracker_filled_posts

    utils.sink_to_parquet(
        lf,
        destination,
        append=False,
    )


if __name__ == "__main__":
    print("Running Estimate Ind CQC job")

    args = utils.get_args(
        (
            "--bucket_name",
            "The bucket (name only) in which to source the model predictions dataset from",
        ),
        (
            "--imputed_ind_cqc_data_source",
            "S3 URI to read imputed ASC-WDS and PIR data from",
        ),
        (
            "--destination",
            "S3 URI to save estimated filled posts data to",
        ),
    )

    main(
        bucket_name=args.bucket_name,
        imputed_ind_cqc_data_source=args.imputed_ind_cqc_data_source,
        destination=args.destination,
    )

    print("Finished Estimate Ind CQC job")
