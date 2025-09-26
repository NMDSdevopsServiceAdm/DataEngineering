import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.care_homes import (
    model_care_homes,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.non_res_with_and_without_dormancy_combined import (
    combine_non_res_with_and_without_dormancy_models,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.non_res_with_dormancy import (
    model_non_res_with_dormancy,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.non_res_without_dormancy import (
    model_non_res_without_dormancy,
)
from projects._03_independent_cqc.utils.utils.utils import (
    allocate_primary_service_type_second_level,
    merge_columns_in_order,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

ind_cqc_columns = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.specialist_generalist_other_dementia,
    IndCQC.specialist_generalist_other_lda,
    IndCQC.specialist_generalist_other_mh,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.number_of_beds_banded,
    IndCQC.imputed_gac_service_types,
    IndCQC.imputed_registration_date,
    IndCQC.related_location,
    IndCQC.time_registered,
    IndCQC.time_since_dormant,
    IndCQC.registered_manager_names,
    IndCQC.ct_non_res_import_date,
    IndCQC.ct_non_res_care_workers_employed,
    IndCQC.ct_non_res_care_workers_employed_imputed,
    IndCQC.ct_care_home_import_date,
    IndCQC.ct_care_home_total_employed,
    IndCQC.ct_care_home_total_employed_cleaned,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.cqc_pir_import_date,
    IndCQC.pir_people_directly_employed_dedup,
    IndCQC.pir_filled_posts_model,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.contemporary_cssr,
    IndCQC.contemporary_region,
    IndCQC.contemporary_sub_icb,
    IndCQC.contemporary_icb,
    IndCQC.contemporary_icb_region,
    IndCQC.contemporary_ccg,
    IndCQC.posts_rolling_average_model,
    IndCQC.imputed_filled_post_model,
    IndCQC.imputed_filled_posts_per_bed_ratio_model,
    IndCQC.unix_time,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    imputed_ind_cqc_data_source: str,
    care_home_features_source: str,
    care_home_model_source: str,
    non_res_with_dormancy_features_source: str,
    non_res_with_dormancy_model_source: str,
    non_res_without_dormancy_features_source: str,
    non_res_without_dormancy_model_source: str,
    estimated_ind_cqc_destination: str,
    ml_model_metrics_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 2000")

    estimate_filled_posts_df = utils.read_from_parquet(
        imputed_ind_cqc_data_source,
        ind_cqc_columns,
    )
    care_home_features_df = utils.read_from_parquet(care_home_features_source)
    non_res_with_dormancy_features_df = utils.read_from_parquet(
        non_res_with_dormancy_features_source
    )
    non_res_without_dormancy_features_df = utils.read_from_parquet(
        non_res_without_dormancy_features_source
    )

    estimate_filled_posts_df = model_care_homes(
        estimate_filled_posts_df,
        care_home_features_df,
        care_home_model_source,
        ml_model_metrics_destination,
    )

    estimate_filled_posts_df = model_non_res_with_dormancy(
        estimate_filled_posts_df,
        non_res_with_dormancy_features_df,
        non_res_with_dormancy_model_source,
        ml_model_metrics_destination,
    )
    estimate_filled_posts_df = model_non_res_without_dormancy(
        estimate_filled_posts_df,
        non_res_without_dormancy_features_df,
        non_res_without_dormancy_model_source,
        ml_model_metrics_destination,
    )

    estimate_filled_posts_df = combine_non_res_with_and_without_dormancy_models(
        estimate_filled_posts_df
    )

    estimate_filled_posts_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_filled_posts_df,
        IndCQC.ascwds_pir_merged,
        IndCQC.care_home_model,
        IndCQC.imputed_posts_care_home_model,
        care_home=True,
    )

    estimate_filled_posts_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_filled_posts_df,
        IndCQC.ascwds_pir_merged,
        IndCQC.non_res_combined_model,
        IndCQC.imputed_posts_non_res_combined_model,
        care_home=False,
    )

    estimate_filled_posts_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_filled_posts_df,
        IndCQC.pir_filled_posts_model,
        IndCQC.non_res_combined_model,
        IndCQC.imputed_pir_filled_posts_model,
        care_home=False,
    )

    estimate_filled_posts_df = merge_columns_in_order(
        estimate_filled_posts_df,
        [
            IndCQC.ascwds_pir_merged,
            IndCQC.imputed_posts_care_home_model,
            IndCQC.care_home_model,
            IndCQC.imputed_posts_non_res_combined_model,
            IndCQC.imputed_pir_filled_posts_model,
            IndCQC.non_res_combined_model,
            IndCQC.posts_rolling_average_model,
        ],
        IndCQC.estimate_filled_posts,
        IndCQC.estimate_filled_posts_source,
    )

    estimate_filled_posts_df = allocate_primary_service_type_second_level(
        estimate_filled_posts_df
    )

    print(f"Exporting as parquet to {estimated_ind_cqc_destination}")

    utils.write_to_parquet(
        estimate_filled_posts_df,
        estimated_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        imputed_ind_cqc_data_source,
        care_home_features_source,
        care_home_model_source,
        non_res_with_dormancy_features_source,
        non_res_with_dormancy_model_source,
        non_res_without_dormancy_features_source,
        non_res_without_dormancy_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    ) = utils.collect_arguments(
        (
            "--imputed_ind_cqc_data_source",
            "Source s3 directory for imputed ASCWDS and PIR dataset",
        ),
        (
            "--care_home_features_source",
            "Source s3 directory for care home features dataset",
        ),
        (
            "--care_home_model_source",
            "Source s3 directory for the care home ML model",
        ),
        (
            "--non_res_with_dormancy_features_source",
            "Source s3 directory for non res with dormancy features dataset",
        ),
        (
            "--non_res_with_dormancy_model_source",
            "Source s3 directory for the non res with dormancy ML model",
        ),
        (
            "--non_res_without_dormancy_features_source",
            "Source s3 directory for non res without dormancy features dataset",
        ),
        (
            "--non_res_without_dormancy_model_source",
            "Source s3 directory for the non res without dormancy ML model",
        ),
        (
            "--estimated_ind_cqc_destination",
            "Destination s3 directory for outputting estimates for filled posts",
        ),
        (
            "--ml_model_metrics_destination",
            "Destination s3 directory for outputting metrics from the ML models",
        ),
    )

    main(
        imputed_ind_cqc_data_source,
        care_home_features_source,
        care_home_model_source,
        non_res_with_dormancy_features_source,
        non_res_with_dormancy_model_source,
        non_res_without_dormancy_features_source,
        non_res_without_dormancy_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    )
