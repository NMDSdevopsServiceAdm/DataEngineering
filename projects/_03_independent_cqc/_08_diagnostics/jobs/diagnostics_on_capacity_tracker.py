import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame

from polars_utils.logger import get_logger
from projects._03_independent_cqc._08_diagnostics.utils import (
    diagnostics_utils as dUtils,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

logger = get_logger(__name__)

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
    IndCQC.ct_care_home_total_employed,
    IndCQC.ct_care_home_total_employed_imputed,
    IndCQC.ct_non_res_care_workers_employed,
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
    logger.info("Creating diagnostics for capacity tracker data")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )

    care_home_diagnostics_df = run_diagnostics(
        filled_posts_df, CareHome.care_home, IndCQC.ct_care_home_total_employed_imputed
    )
    non_res_diagnostics_df = run_diagnostics(
        filled_posts_df, CareHome.not_care_home, IndCQC.ct_non_res_filled_post_estimate
    )

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


def run_diagnostics(
    df: DataFrame, care_home_value: str, col_for_diagnostics: str
) -> DataFrame:
    """
    Controls the steps to generate the diagnostic data frame using capacity tracker data as a comparison.

    Args:
        df (DataFrame): A dataframe containing pipeline estimates.
        care_home_value (str): The categorical value to filter the care_home column by.
        col_for_diagnostics (str): The column name to use for diagnostics comparison.

    Returns:
        DataFrame: A dataframe containing diagnostic data for locations using capacity tracker values.
    """
    filtered_df = utils.select_rows_with_value(
        df, IndCQC.care_home, value_to_keep=care_home_value
    )

    list_of_models = dUtils.create_list_of_models()
    restructured_df = dUtils.restructure_dataframe_to_column_wise(
        filtered_df, col_for_diagnostics, list_of_models
    )
    known_estimates_df = dUtils.filter_to_known_values(
        restructured_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    diagnostics_df = dUtils.calculate_distribution_metrics(known_estimates_df, window)
    diagnostics_df = dUtils.calculate_residuals(diagnostics_df, col_for_diagnostics)
    diagnostics_df = dUtils.calculate_aggregate_residuals(
        diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return diagnostics_df


if __name__ == "__main__":
    logger.info("Spark job 'diagnostics_on_capacity_tracker_data' starting...")
    logger.info(f"Job parameters: {sys.argv}")

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

    logger.info("Spark job 'diagnostics_on_capacity_tracker_data' complete")
