import sys

from pyspark.sql import DataFrame, functions as F

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
from utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from utils.estimate_filled_posts.models.primary_service_rate_of_change import (
    model_primary_service_rate_of_change,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list,
)

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
    IndCQC.non_res_pir_linear_regression_model,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_with_dormancy_model,
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
number_of_days_in_rolling_average: int = 185  # Note: using 185 as a proxy for 6 months


def main(
    estimate_filled_posts_source,
    capacity_tracker_care_home_source,
    capacity_tracker_non_res_source,
    care_home_diagnostics_destination,
    care_home_summary_diagnostics_destination,
    non_res_diagnostics_destination,
    non_res_summary_diagnostics_destination,
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
        filled_posts_df,
        ct_care_home_df,
        CTCHClean.agency_and_non_agency_total_employed,
    )
    non_res_diagnostics_df = run_diagnostics_for_non_residential(
        filled_posts_df, ct_non_res_df, CTNRClean.capacity_tracker_filled_post_estimate
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


def run_diagnostics_for_care_homes(
    filled_posts_df: DataFrame, ct_care_home_df: DataFrame, column_for_comparison: str
) -> DataFrame:
    """
    Controls the steps to generate the care home diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df (DataFrame): A dataframe containing pipeline estimates.
        ct_care_home_df (DataFrame): A dataframe containing cleaned capacity tracker data for care homes.
        column_for_comparison (str): A column to use from the capacity tracker data for calculating residuals.

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
    care_home_diagnostics_df = model_primary_service_rate_of_change(
        care_home_diagnostics_df,
        CTCHClean.agency_and_non_agency_total_employed,
        number_of_days_in_rolling_average,
        CTCHClean.agency_and_non_agency_total_employed_rate_of_change_trendline,
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


def run_diagnostics_for_non_residential(
    filled_posts_df: DataFrame,
    ct_non_res_df: DataFrame,
    column_for_comparison: str,
) -> DataFrame:
    """
    Controls the steps to generate the non residential diagnostic data frame using capacity tracker data as a comparison.

    Args:
        filled_posts_df (DataFrame): A dataframe containing pipeline estimates.
        ct_non_res_df (DataFrame): A dataframe containing cleaned capacity tracker data for non residential locations.
        column_for_comparison (str): A column to use from the capacity tracker data for calculating residuals.

    Returns:
        DataFrame: A dataframe containing diagnostic data for non residential locations using capacity tracker values.
    """
    filled_posts_df = utils.select_rows_with_value(
        filled_posts_df, IndCQC.care_home, value_to_keep=CareHome.not_care_home
    )
    non_res_diagnostics_df = join_capacity_tracker_data(
        filled_posts_df, ct_non_res_df, care_home=False
    )
    non_res_diagnostics_df = model_primary_service_rate_of_change(
        non_res_diagnostics_df,
        CTNRClean.cqc_care_workers_employed,
        number_of_days_in_rolling_average,
        CTNRClean.cqc_care_workers_employed_rate_of_change_trendline,
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
    non_res_diagnostics_df = (
        populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
            non_res_diagnostics_df,
            [
                CTNRClean.capacity_tracker_all_posts,
                IndCQC.estimate_filled_posts,
            ],
            CTNRClean.capacity_tracker_filled_post_estimate,
            CTNRClean.capacity_tracker_filled_post_estimate_source,
        )
    )
    list_of_models = dUtils.create_list_of_models()
    non_res_diagnostics_df = dUtils.restructure_dataframe_to_column_wise(
        non_res_diagnostics_df, column_for_comparison, list_of_models
    )
    non_res_diagnostics_df = dUtils.filter_to_known_values(
        non_res_diagnostics_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    non_res_diagnostics_df = dUtils.calculate_distribution_metrics(
        non_res_diagnostics_df, window
    )
    non_res_diagnostics_df = dUtils.calculate_residuals(
        non_res_diagnostics_df, column_for_comparison
    )
    non_res_diagnostics_df = dUtils.calculate_aggregate_residuals(
        non_res_diagnostics_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    return non_res_diagnostics_df


def join_capacity_tracker_data(
    filled_posts_df: DataFrame, capacity_tracker_df: DataFrame, care_home: bool
) -> DataFrame:
    """
    Joins capacity tracker dataframes on correct columns depending on whether they are for care home data or non res data.

    Args:
        filled_posts_df (DataFrame): A dataframe containing pipeline estimates.
        capacity_tracker_df (DataFrame): A dataframe containing cleaned capacity tracker data.
        care_home (bool): True if the capaccity tracker data is for care homes, false if it is for non residential.
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
        capacity_tracker_care_home_source,
        capacity_tracker_non_res_source,
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
        (
            "--non_res_summary_diagnostics_destination",
            "A destination directory for outputting summary non residential diagnostics tables.",
        ),
    )

    main(
        estimate_filled_posts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_res_source,
        care_home_diagnostics_destination,
        care_home_summary_diagnostics_destination,
        non_res_diagnostics_destination,
        non_res_summary_diagnostics_destination,
    )

    print("Spark job 'diagnostics_on_capacity_tracker_data' complete")
