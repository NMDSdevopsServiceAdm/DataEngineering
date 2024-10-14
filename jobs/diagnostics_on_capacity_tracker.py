import sys

from pyspark.sql import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_values.categorical_column_values import CareHome

from utils.diagnostics_utils import diagnostics_utils as dUtils

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.care_home,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.extrapolation_non_res_with_dormancy_model,
    IndCQC.extrapolation_rolling_average_model,
    IndCQC.estimate_filled_posts,
    IndCQC.current_region,
    IndCQC.current_cssr,
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
    capacity_tracker_care_home_source,
    capacity_tracker_non_res_source,
    care_home_diagnostics_destination,
    care_home_summary_diagnostics_destination,
    non_res_diagnostics_destination,
):
    print("Creating diagnostics for capacity tracker data")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )
    ct_care_home_df: DataFrame = utils.read_from_parquet(
        capacity_tracker_care_home_source
    )
    ct_non_res_df: DataFrame = utils.read_from_parquet(capacity_tracker_non_res_source)

    care_home_diagnostics_df = run_diagnostics_for_care_homes(
        filled_posts_df, ct_care_home_df, CTCHClean.agency_and_non_agency_total_employed
    )
    non_res_diagnostics_df = run_diagnostics_for_non_residential(
        filled_posts_df, ct_non_res_df
    )

    care_home_summary_df = dUtils.create_summary_diagnostics_table(
        care_home_diagnostics_df
    )

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


def run_diagnostics_for_care_homes(
    filled_posts_df: DataFrame, ct_care_home_df: DataFrame, column_for_comparison: str
) -> DataFrame:
    """
    Controls the steps to generate the care home diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df(Dataframe): A dataframe containing pipeline estimates.
        ct_care_home_df(DataFrame): A dataframe containing cleaned capacity tracker data for care homes.
        column_for_comparison(str): A column to use from the capacity tracker data for calculating residuals.

    Returns:
        DataFrame: A dataframe containing diagnostic data for care homes using capacity tracker values.
    """
    filled_posts_df = utils.select_rows_with_value(
        filled_posts_df, IndCQC.care_home, value_to_keep=CareHome.care_home
    )
    ct_care_home_df = dUtils.filter_to_known_values(
        ct_care_home_df, column_for_comparison
    )
    care_home_diagnostics_df = join_capacity_tracker_data(
        filled_posts_df, ct_care_home_df, care_home=True
    )
    care_home_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        care_home_diagnostics_df, column_for_comparison
    )
    care_home_diagnostics_df = dUtils.filter_to_known_values(
        care_home_diagnostics_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    care_home_diagnostics_df = dUtils.calculate_distribution_metrics(
        care_home_diagnostics_df, window
    )
    care_home_diagnostics_df = dUtils.calculate_residuals(
        care_home_diagnostics_df, column_for_comparison
    )
    care_home_diagnostics_df = dUtils.calculate_aggregate_residuals(
        care_home_diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return care_home_diagnostics_df


def run_diagnostics_for_non_residential(
    filled_posts_df: DataFrame,
    ct_non_res_df: DataFrame,
) -> DataFrame:
    """
    Controls the steps to generate the non residential diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df(Dataframe): A dataframe containing pipeline estimates.
        ct_non_res_df(DataFrame): A dataframe containing cleaned capacity tracker data for non residential locations.

    Returns:
        DataFrame: A dataframe containing diagnostic data for non residential locations using capacity tracker values.
    """
    filled_posts_df = utils.select_rows_with_value(
        filled_posts_df, IndCQC.care_home, value_to_keep=CareHome.not_care_home
    )
    non_res_diagnostics_df = join_capacity_tracker_data(
        filled_posts_df, ct_non_res_df, care_home=False
    )
    return non_res_diagnostics_df


def join_capacity_tracker_data(
    filled_posts_df: DataFrame, capacity_tracker_df: DataFrame, care_home: bool
):
    """
    Joins capacity tracker dataframes on correct columns depending on whether they are for care home data or non res data.

    Args:
        filled_posts_df(Dataframe): A dataframe containing pipeline estimates.
        capacity_tracker_df(DataFrame): A dataframe containing cleaned capacity tracker data.
        care_home(bool): True if the capaccity tracker data is for care homes, false if it is for non residential.
    Returns:
        DataFrame: A dataframe containing filled posts data with capacity tracker data joined in.
    """
    capacity_tracker_df = capacity_tracker_df.drop(*partition_keys)
    if care_home == True:
        filled_posts_df = cUtils.add_aligned_date_column(
            filled_posts_df,
            capacity_tracker_df,
            IndCQC.cqc_location_import_date,
            CTCHClean.capacity_tracker_import_date,
        )
        capacity_tracker_df = capacity_tracker_df.withColumnRenamed(
            CTCHClean.cqc_id, IndCQC.location_id
        )
    else:
        filled_posts_df = cUtils.add_aligned_date_column(
            filled_posts_df,
            capacity_tracker_df,
            IndCQC.cqc_location_import_date,
            CTNRClean.capacity_tracker_import_date,
        )
        capacity_tracker_df = capacity_tracker_df.withColumnRenamed(
            CTNRClean.cqc_id, IndCQC.location_id
        )

    joined_df = filled_posts_df.join(
        capacity_tracker_df,
        [IndCQC.location_id, CTCHClean.capacity_tracker_import_date],
        how="left",
    )
    return joined_df


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_capacity_tracker_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_res_source,
        care_home_diagnostics_destination,
        care_home_summary_diagnostics_destination,
        non_res_diagnostics_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_filled_posts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for capacity tracker care home cleaned data",
        ),
        (
            "--capacity_tracker_non_res_source",
            "Source s3 directory for capacity tracker non residential cleaned data",
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
    )

    main(
        estimate_filled_posts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_res_source,
        care_home_diagnostics_destination,
        care_home_summary_diagnostics_destination,
        non_res_diagnostics_destination,
    )

    print("Spark job 'diagnostics_on_capacity_tracker_data' complete")
