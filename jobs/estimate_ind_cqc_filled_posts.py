import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.estimate_filled_posts.models.care_homes import model_care_homes
from utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from utils.estimate_filled_posts.models.non_res_with_dormancy import (
    model_non_res_with_dormancy,
)
from utils.estimate_filled_posts.models.non_res_without_dormancy import (
    model_non_res_without_dormancy,
)
from utils.estimate_filled_posts.models.non_res_pir_linear_regression import (
    model_non_res_pir_linear_regression,
)

from utils.ind_cqc_filled_posts_utils.utils import (
    populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list,
)

estimate_missing_ascwds_columns = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_gac_service_types,
    IndCQC.imputed_registration_date,
    IndCQC.related_location,
    IndCQC.cqc_pir_import_date,
    IndCQC.people_directly_employed_dedup,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.rolling_average_model,
    IndCQC.imputed_posts_rolling_avg_model,
    IndCQC.imputed_non_res_people_directly_employed,
    IndCQC.imputed_ratio_rolling_avg_model,
    IndCQC.rolling_average_model_filled_posts_per_bed_ratio,
    IndCQC.unix_time,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    estimate_missing_ascwds_filled_posts_data_source: str,
    care_home_features_source: str,
    care_home_model_source: str,
    non_res_with_dormancy_features_source: str,
    non_res_with_dormancy_model_source: str,
    non_res_without_dormancy_features_source: str,
    non_res_without_dormancy_model_source: str,
    non_res_pir_linear_regression_features_source: str,
    non_res_pir_linear_regression_model_source: str,
    estimated_ind_cqc_destination: str,
    ml_model_metrics_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 2000")

    estimate_missing_ascwds_df = utils.read_from_parquet(
        estimate_missing_ascwds_filled_posts_data_source,
        estimate_missing_ascwds_columns,
    )
    care_home_features_df = utils.read_from_parquet(care_home_features_source)
    non_res_with_dormancy_features_df = utils.read_from_parquet(
        non_res_with_dormancy_features_source
    )
    non_res_without_dormancy_features_df = utils.read_from_parquet(
        non_res_without_dormancy_features_source
    )
    non_res_pir_linear_regression_features_df = utils.read_from_parquet(
        non_res_pir_linear_regression_features_source
    )

    estimate_filled_posts_df = model_care_homes(
        estimate_missing_ascwds_df,
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

    estimate_filled_posts_df = model_non_res_pir_linear_regression(
        estimate_filled_posts_df,
        non_res_pir_linear_regression_features_df,
        non_res_pir_linear_regression_model_source,
        ml_model_metrics_destination,
    )

    estimate_filled_posts_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_filled_posts_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.care_home_model,
        IndCQC.imputed_posts_care_home_model,
        care_home=True,
    )

    estimate_filled_posts_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_filled_posts_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.non_res_with_dormancy_model,
        IndCQC.imputed_posts_non_res_with_dormancy_model,
        care_home=False,
    )

    # TODO: add imputation for other non res models

    estimate_filled_posts_df = (
        populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
            estimate_filled_posts_df,
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                IndCQC.imputed_posts_care_home_model,
                IndCQC.care_home_model,
                IndCQC.imputed_posts_rolling_avg_model,
                IndCQC.imputed_posts_non_res_with_dormancy_model,
                IndCQC.non_res_with_dormancy_model,
                IndCQC.non_res_without_dormancy_model,
                IndCQC.non_res_pir_linear_regression_model,
                IndCQC.rolling_average_model,
            ],
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
        )
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
        estimate_missing_ascwds_filled_posts_data_source,
        care_home_features_source,
        care_home_model_source,
        non_res_with_dormancy_features_source,
        non_res_with_dormancy_model_source,
        non_res_without_dormancy_features_source,
        non_res_without_dormancy_model_source,
        non_res_pir_linear_regression_features_source,
        non_res_pir_linear_regression_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_missing_ascwds_filled_posts_data_source",
            "Source s3 directory for estimate_missing_ascwds_filled_posts",
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
            "--non_res_pir_linear_regression_features_source",
            "Source s3 directory for non res pir linear regression features dataset",
        ),
        (
            "--non_res_pir_linear_regression_model_source",
            "Source s3 directory for the non res pir linear regression model",
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
        estimate_missing_ascwds_filled_posts_data_source,
        care_home_features_source,
        care_home_model_source,
        non_res_with_dormancy_features_source,
        non_res_with_dormancy_model_source,
        non_res_without_dormancy_features_source,
        non_res_without_dormancy_model_source,
        non_res_pir_linear_regression_features_source,
        non_res_pir_linear_regression_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    )
