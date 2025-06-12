import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
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
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_trendline import (
    model_primary_service_rate_of_change_trendline,
)
from utils.ind_cqc_filled_posts_utils.utils import merge_columns_in_order

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
    IndCQC.number_of_beds,
    IndCQC.current_region,
    IndCQC.current_cssr,
    IndCQC.unix_time,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
absolute_value_cutoff: float = 10.0
percentage_value_cutoff: float = 0.25
standardised_value_cutoff: float = 1.0
number_of_days_in_window: int = 95  # Note: using 95 as a proxy for 3 months
max_number_of_days_to_interpolate_between: int = 185  # proxy for 6 months


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
    care_home_diagnostics_df = model_primary_service_rate_of_change_trendline(
        care_home_diagnostics_df,
        CTCHClean.agency_and_non_agency_total_employed,
        number_of_days_in_window,
        CTCHClean.agency_and_non_agency_total_employed_rate_of_change_trendline,
        max_number_of_days_to_interpolate_between,
    )
    care_home_diagnostics_df = model_imputation_with_extrapolation_and_interpolation(
        care_home_diagnostics_df,
        CTCHClean.agency_and_non_agency_total_employed,
        CTCHClean.agency_and_non_agency_total_employed_rate_of_change_trendline,
        CTCHClean.agency_and_non_agency_total_employed_imputed,
        care_home=True,
    )
    list_of_models = dUtils.create_list_of_models()
    care_home_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        care_home_diagnostics_df,
        CTCHClean.agency_and_non_agency_total_employed_imputed,
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
        CTCHClean.agency_and_non_agency_total_employed_imputed,
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
    non_res_diagnostics_df = model_primary_service_rate_of_change_trendline(
        non_res_diagnostics_df,
        CTNRClean.cqc_care_workers_employed,
        number_of_days_in_window,
        CTNRClean.cqc_care_workers_employed_rate_of_change_trendline,
        max_number_of_days_to_interpolate_between,
    )
    non_res_diagnostics_df = model_imputation_with_extrapolation_and_interpolation(
        non_res_diagnostics_df,
        CTNRClean.cqc_care_workers_employed,
        CTNRClean.cqc_care_workers_employed_rate_of_change_trendline,
        CTNRClean.cqc_care_workers_employed_imputed,
        care_home=False,
    )
    care_worker_ratio = calculate_care_worker_ratio(
        non_res_diagnostics_df,
    )
    non_res_diagnostics_df = convert_to_all_posts_using_ratio(
        non_res_diagnostics_df, care_worker_ratio
    )
    non_res_diagnostics_df = merge_columns_in_order(
        non_res_diagnostics_df,
        [
            CTNRClean.capacity_tracker_all_posts,
            IndCQC.estimate_filled_posts,
        ],
        CTNRClean.capacity_tracker_filled_post_estimate,
        CTNRClean.capacity_tracker_filled_post_estimate_source,
    )
    list_of_models = dUtils.create_list_of_models()
    non_res_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        non_res_diagnostics_df,
        CTNRClean.capacity_tracker_filled_post_estimate,
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
        non_res_diagnostics_df, CTNRClean.capacity_tracker_filled_post_estimate
    )
    non_res_diagnostics_df = dUtils.calculate_aggregate_residuals(
        non_res_diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return non_res_diagnostics_df


def calculate_care_worker_ratio(df: DataFrame) -> float:
    """
    Calculate the overall ratio of care workers to all posts and print it.

    Args:
        df (DataFrame): A dataframe containing the columns estimate_filled_posts and cqc_care_workers_employed_imputed.
    Returns:
        float: A float representing the ratio between care workers and all posts.
    """
    df = df.where(
        (df[CTNRClean.cqc_care_workers_employed_imputed].isNotNull())
        & (df[IndCQC.estimate_filled_posts].isNotNull())
    )
    total_care_workers = df.agg(
        F.sum(df[CTNRClean.cqc_care_workers_employed_imputed])
    ).collect()[0][0]
    total_posts = df.agg(F.sum(df[IndCQC.estimate_filled_posts])).collect()[0][0]
    care_worker_ratio = total_care_workers / total_posts
    print(f"The care worker ratio used is: {care_worker_ratio}.")
    return care_worker_ratio


def convert_to_all_posts_using_ratio(
    df: DataFrame, care_worker_ratio: float
) -> DataFrame:
    """
    Convert the cqc_care_workers_employed figures to all workers.

    Args:
        df (DataFrame): A dataframe with non res capacity tracker data.
        care_worker_ratio (float): The ratio of all care workers divided by all posts.

    Returns:
        DataFrame: A dataframe with a new column containing the all-workers estimate.
    """
    df = df.withColumn(
        CTNRClean.capacity_tracker_all_posts,
        F.col(CTNRClean.cqc_care_workers_employed_imputed) / care_worker_ratio,
    )
    return df


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
