import pyspark.sql.functions as F
import pyspark.sql
from pyspark.sql.types import StringType
from pyspark.ml.feature import Bucketizer
from dataclasses import dataclass


@dataclass
class ColNames:
    locationid: str = "locationid"
    snapshot_date: str = "snapshot_date"
    primary_service_type: str = "primary_service_type"
    job_count_unfiltered: str = "job_count_unfiltered"
    number_of_beds: str = "number_of_beds"
    number_of_beds_banded: str = "number_of_beds_banded"
    jobs_per_bed_ratio: str = "jobs_per_bed_ratio"
    avg_jobs_per_bed_ratio: str = "avg_jobs_per_bed_ratio"
    expected_jobs: str = "expected_jobs"
    residual: str = "residual"
    standardised_residual: str = "standardised_residual"
    job_count: str = "job_count"


@dataclass
class NumericalValues:
    DECIMAL_PLACES_TO_ROUND_TO: int = 5
    PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS: float = 0.05


def care_home_jobs_per_bed_ratio_outliers(
    input_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    numerical_value = NumericalValues()

    care_homes_df = select_relevant_data(input_df)
    data_not_relevant_to_filter_df = select_data_not_in_subset_df(
        input_df, care_homes_df
    )

    data_to_filter_df = calculate_jobs_per_bed_ratio(care_homes_df)

    data_to_filter_df = create_banded_bed_count_column(data_to_filter_df)

    expected_jobs_per_banded_bed_count_df = calculate_average_jobs_per_banded_bed_count(
        data_to_filter_df
    )

    data_to_filter_df = calculate_standardised_residuals(
        data_to_filter_df, expected_jobs_per_banded_bed_count_df
    )

    data_to_filter_df = calculate_standardised_residual_cutoffs(
        data_to_filter_df,
        numerical_value.PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS,
        "lower_percentile",
        "upper_percentile",
    )

    care_homes_within_standardised_residual_cutoff_df = create_filtered_job_count_df(
        data_to_filter_df
    )

    care_homes_with_filtered_col_df = join_filtered_col_into_care_home_df(
        care_homes_df, care_homes_within_standardised_residual_cutoff_df
    )

    data_not_relevant_to_filter_df = (
        add_job_counts_without_filtering_to_data_outside_of_this_filter(
            data_not_relevant_to_filter_df
        )
    )

    output_df = combine_dataframes(
        care_homes_with_filtered_col_df, data_not_relevant_to_filter_df
    )

    return output_df


def select_relevant_data(input_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    output_df = input_df.where(
        (F.col(column_name.primary_service_type) == "Care home without nursing")
        | (F.col(column_name.primary_service_type) == "Care home with nursing")
    )
    output_df = output_df.where(
        F.col(column_name.number_of_beds).isNotNull()
        & (F.col(column_name.number_of_beds) > 0)
    )
    output_df = output_df.where(
        F.col(column_name.job_count_unfiltered).isNotNull()
        & (F.col(column_name.job_count_unfiltered) > 0.0)
    )

    return output_df


def select_data_not_in_subset_df(
    complete_df: pyspark.sql.DataFrame, subset_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    output_df = complete_df.exceptAll(subset_df)

    return output_df


def calculate_jobs_per_bed_ratio(
    input_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    input_df = input_df.withColumn(
        column_name.jobs_per_bed_ratio,
        F.col(column_name.job_count_unfiltered) / F.col(column_name.number_of_beds),
    )

    return input_df


def create_banded_bed_count_column(
    input_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    set_banded_boundaries = Bucketizer(
        splits=[0, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
        inputCol=column_name.number_of_beds,
        outputCol=column_name.number_of_beds_banded,
    )

    input_df = set_banded_boundaries.setHandleInvalid("keep").transform(input_df)

    return input_df


def calculate_average_jobs_per_banded_bed_count(
    input_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    output_df = input_df.groupBy(F.col(column_name.number_of_beds_banded)).agg(
        F.avg(column_name.jobs_per_bed_ratio).alias(column_name.avg_jobs_per_bed_ratio)
    )

    return output_df


def calculate_standardised_residuals(
    df: pyspark.sql.DataFrame,
    expected_jobs_per_banded_bed_count_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    df = calculate_expected_jobs_based_on_number_of_beds(
        df, expected_jobs_per_banded_bed_count_df
    )
    df = calculate_job_count_residuals(df)
    df = calculate_job_count_standardised_residual(df)

    return df


def calculate_expected_jobs_based_on_number_of_beds(
    df: pyspark.sql.DataFrame,
    expected_jobs_per_banded_bed_count_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    df = df.join(
        expected_jobs_per_banded_bed_count_df, column_name.number_of_beds_banded, "left"
    )

    df = df.withColumn(
        column_name.expected_jobs,
        F.col(column_name.number_of_beds) * F.col(column_name.avg_jobs_per_bed_ratio),
    )

    return df


def calculate_job_count_residuals(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    df = df.withColumn(
        column_name.residual,
        F.col(column_name.job_count_unfiltered) - F.col(column_name.expected_jobs),
    )

    return df


def calculate_job_count_standardised_residual(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    df = df.withColumn(
        column_name.standardised_residual,
        F.col(column_name.residual) / F.sqrt(F.col(column_name.expected_jobs)),
    )

    return df


def calculate_standardised_residual_cutoffs(
    df: pyspark.sql.DataFrame,
    percentage_of_data_to_filter_out: float,
    lower_percentile_name: str,
    upper_percentile_name: str,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    lower_percentile = percentage_of_data_to_filter_out / 2
    upper_percentile = 1 - (percentage_of_data_to_filter_out / 2)

    df = calculate_percentile(
        df, column_name.standardised_residual, lower_percentile, lower_percentile_name
    )
    df = calculate_percentile(
        df, column_name.standardised_residual, upper_percentile, upper_percentile_name
    )

    return df


def calculate_percentile(
    df: pyspark.sql.DataFrame, col_name: str, percentile_value: float, alias: str
) -> pyspark.sql.DataFrame:

    df = df.withColumn("temp_col_for_joining", F.lit(0))
    percentile_df = df.groupBy("temp_col_for_joining").agg(
        F.expr("percentile(" + col_name + ", array(" + str(percentile_value) + "))")[
            0
        ].alias(alias)
    )

    df = df.join(percentile_df, "temp_col_for_joining", "left").drop(
        "temp_col_for_joining"
    )

    return df


def create_filtered_job_count_df(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    within_boundary_df = df.filter(
        (F.col(column_name.standardised_residual) > F.col("lower_percentile"))
        & (F.col(column_name.standardised_residual) < F.col("upper_percentile"))
    )

    within_boundary_df = within_boundary_df.withColumn(
        column_name.job_count, F.col(column_name.job_count_unfiltered)
    )

    output_df = within_boundary_df.select(
        column_name.locationid, column_name.snapshot_date, column_name.job_count
    )

    return output_df


def join_filtered_col_into_care_home_df(
    df: pyspark.sql.DataFrame, df_with_filtered_column: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    return df.join(
        df_with_filtered_column,
        [column_name.locationid, column_name.snapshot_date],
        "left",
    )


def add_job_counts_without_filtering_to_data_outside_of_this_filter(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    return df.withColumn(column_name.job_count, F.col(column_name.job_count_unfiltered))


def combine_dataframes(
    first_df: pyspark.sql.DataFrame, second_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    output_df = first_df.unionByName(second_df)

    return output_df
