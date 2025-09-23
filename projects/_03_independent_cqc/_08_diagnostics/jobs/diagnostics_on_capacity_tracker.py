import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame

from projects._03_independent_cqc._08_diagnostics.utils import (
    diagnostics_utils as dUtils,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.care_home,
    IndCQC.primary_service_type,
    IndCQC.posts_rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.imputed_filled_post_model,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.non_res_combined_model,
    IndCQC.imputed_pir_filled_posts_model,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_combined_model,
    IndCQC.estimate_filled_posts,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ct_non_res_filled_post_estimate,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
absolute_value_cutoff: float = 10.0
percentage_value_cutoff: float = 0.25
standardised_value_cutoff: float = 1.0


def main(
    estimate_filled_posts_source,
    care_home_diagnostics_destination,
    care_home_summary_diagnostics_destination,
    non_res_diagnostics_destination,
    non_res_summary_diagnostics_destination,
):
    print("Creating diagnostics for capacity tracker data")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )

    care_home_diagnostics_df = run_diagnostics_for_care_homes(filled_posts_df)
    non_res_diagnostics_df = run_diagnostics_for_non_residential(filled_posts_df)

    care_home_summary_df = dUtils.create_summary_diagnostics_table(
        care_home_diagnostics_df
    )
    non_res_summary_df = dUtils.create_summary_diagnostics_table(non_res_diagnostics_df)

    utils.write_to_parquet(
        care_home_diagnostics_df,
        care_home_diagnostics_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )
    utils.write_to_parquet(
        care_home_summary_df,
        care_home_summary_diagnostics_destination,
        mode="overwrite",
        partitionKeys=[IndCQC.primary_service_type],
    )
    utils.write_to_parquet(
        non_res_diagnostics_df,
        non_res_diagnostics_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )
    utils.write_to_parquet(
        non_res_summary_df,
        non_res_summary_diagnostics_destination,
        mode="overwrite",
        partitionKeys=[IndCQC.primary_service_type],
    )


def run_diagnostics_for_care_homes(filled_posts_df: DataFrame) -> DataFrame:
    """
    Controls the steps to generate the care home diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df (DataFrame): A dataframe containing pipeline estimates.

    Returns:
        DataFrame: A dataframe containing diagnostic data for care homes using capacity tracker values.
    """
    care_home_diagnostics_df = utils.select_rows_with_value(
        filled_posts_df, IndCQC.care_home, value_to_keep=CareHome.care_home
    )
    list_of_models = dUtils.create_list_of_models()
    care_home_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        care_home_diagnostics_df,
        IndCQC.ct_care_home_total_employed_imputed,
        list_of_models,
    )
    care_home_diagnostics_df = dUtils.filter_to_known_values(
        care_home_diagnostics_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    care_home_diagnostics_df = dUtils.calculate_distribution_metrics(
        care_home_diagnostics_df, window
    )
    care_home_diagnostics_df = dUtils.calculate_residuals(
        care_home_diagnostics_df,
        IndCQC.ct_care_home_total_employed_imputed,
    )
    care_home_diagnostics_df = dUtils.calculate_aggregate_residuals(
        care_home_diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return care_home_diagnostics_df


def run_diagnostics_for_non_residential(filled_posts_df: DataFrame) -> DataFrame:
    """
    Controls the steps to generate the non residential diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df (DataFrame): A dataframe containing pipeline estimates.

    Returns:
        DataFrame: A dataframe containing diagnostic data for non residential locations using capacity tracker values.
    """
    non_res_diagnostics_df = utils.select_rows_with_value(
        filled_posts_df, IndCQC.care_home, value_to_keep=CareHome.not_care_home
    )

    list_of_models = dUtils.create_list_of_models()
    non_res_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        non_res_diagnostics_df,
        IndCQC.ct_non_res_filled_post_estimate,
        list_of_models,
    )
    non_res_diagnostics_df = dUtils.filter_to_known_values(
        non_res_diagnostics_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    non_res_diagnostics_df = dUtils.calculate_distribution_metrics(
        non_res_diagnostics_df, window
    )
    non_res_diagnostics_df = dUtils.calculate_residuals(
        non_res_diagnostics_df, IndCQC.ct_non_res_filled_post_estimate
    )
    non_res_diagnostics_df = dUtils.calculate_aggregate_residuals(
        non_res_diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return non_res_diagnostics_df


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_capacity_tracker_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        care_home_diagnostics_destination,
        care_home_summary_diagnostics_destination,
        non_res_diagnostics_destination,
        non_res_summary_diagnostics_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_filled_posts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--care_home_diagnostics_destination",
            "A destination directory for outputting full care home diagnostics tables.",
        ),
        (
            "--care_home_summary_diagnostics_destination",
            "A destination directory for outputting summary care home diagnostics tables.",
        ),
        (
            "--non_res_diagnostics_destination",
            "A destination directory for outputting full non_residential diagnostics tables.",
        ),
        (
            "--non_res_summary_diagnostics_destination",
            "A destination directory for outputting summary non residential diagnostics tables.",
        ),
    )

    main(
        estimate_filled_posts_source,
        care_home_diagnostics_destination,
        care_home_summary_diagnostics_destination,
        non_res_diagnostics_destination,
        non_res_summary_diagnostics_destination,
    )

    print("Spark job 'diagnostics_on_capacity_tracker_data' complete")
