import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.estimate_filled_posts.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from utils.estimate_filled_posts.models.extrapolation import model_extrapolation
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.estimate_filled_posts.models.care_homes import model_care_homes

from utils.ind_cqc_filled_posts_utils.utils import (
    populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list,
)

cleaned_ind_cqc_columns = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.number_of_beds,
    IndCQC.cqc_pir_import_date,
    IndCQC.people_directly_employed,
    IndCQC.people_directly_employed_dedup,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

NUMBER_OF_DAYS_IN_ROLLING_AVERAGE = 88  # Note: using 88 as a proxy for 3 months


def main(
    cleaned_ind_cqc_source: str,
    care_home_features_source: str,
    care_home_model_source: str,
    estimated_ind_cqc_destination: str,
    ml_model_metrics_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(
        cleaned_ind_cqc_source, cleaned_ind_cqc_columns
    )
    care_home_features_df = utils.read_from_parquet(care_home_features_source)

    cleaned_ind_cqc_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )

    cleaned_ind_cqc_df = model_primary_service_rolling_average(
        cleaned_ind_cqc_df, NUMBER_OF_DAYS_IN_ROLLING_AVERAGE
    )

    cleaned_ind_cqc_df = model_interpolation(cleaned_ind_cqc_df)

    cleaned_ind_cqc_df = model_care_homes(
        cleaned_ind_cqc_df,
        care_home_features_df,
        care_home_model_source,
        ml_model_metrics_destination,
    )

    cleaned_ind_cqc_df = model_extrapolation(cleaned_ind_cqc_df, IndCQC.care_home_model)
    cleaned_ind_cqc_df = model_extrapolation(
        cleaned_ind_cqc_df, IndCQC.rolling_average_model
    )

    cleaned_ind_cqc_df = (
        populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
            cleaned_ind_cqc_df,
            [
                IndCQC.ascwds_filled_posts_dedup_clean,
                IndCQC.interpolation_model,
                IndCQC.extrapolation_model,
                IndCQC.care_home_model,
                IndCQC.rolling_average_model,
            ],
        )
    )

    print(f"Exporting as parquet to {estimated_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        care_home_features_source,
        care_home_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
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
            "--estimated_ind_cqc_destination",
            "Destination s3 directory for outputting estimates for filled posts",
        ),
        (
            "--ml_model_metrics_destination",
            "Destination s3 directory for outputting metrics from the ML models",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        care_home_features_source,
        care_home_model_source,
        estimated_ind_cqc_destination,
        ml_model_metrics_destination,
    )
