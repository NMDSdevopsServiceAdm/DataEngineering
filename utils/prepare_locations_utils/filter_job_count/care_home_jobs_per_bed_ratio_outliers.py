import pyspark.sql.functions as F
import pyspark.sql
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.feature import Bucketizer
from dataclasses import dataclass


@dataclass
class ColNames:
    locationid: str = "locationid"
    snapshot_date: str = "snapshot_date"
    carehome: str = "carehome"
    registration_status: str = "registration_status"
    number_of_beds: str = "number_of_beds"
    number_of_beds_banded: str = "number_of_beds_banded"
    jobs_per_bed_ratio: str = "jobs_per_bed_ratio"
    avg_jobs_per_bed_ratio: str = "avg_jobs_per_bed_ratio"
    expected_jobs: str = "expected_jobs"
    residual: str = "residual"
    standardised_residual: str = "standardised_residual"


@dataclass
class NumericalValues:
    DECIMAL_PLACES_TO_ROUND_TO: int = 5
    PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS: float = 0.05


def care_home_jobs_per_bed_ratio_outliers(
    input_df: pyspark.sql.DataFrame, column_to_filter: str, filtered_col_name: str
) -> pyspark.sql.DataFrame:

    numerical_value = NumericalValues()

    care_homes_df = select_relevant_data(input_df, column_to_filter)
    data_not_relevant_to_filter_df = select_data_not_in_subset_df(
        input_df, care_homes_df
    )

    data_to_filter_df = calculate_jobs_per_bed_ratio(care_homes_df, column_to_filter)

    data_to_filter_df = create_banded_bed_count_column(data_to_filter_df)

    expected_jobs_per_banded_bed_count_df = calculate_average_jobs_per_banded_bed_count(
        data_to_filter_df
    )

    data_to_filter_df = calculate_standardised_residuals(
        data_to_filter_df, expected_jobs_per_banded_bed_count_df, column_to_filter
    )

    (
        lower_standardised_residual_cutoff,
        upper_standardised_residual_cutoff,
    ) = calculate_standardised_residual_cutoffs(
        data_to_filter_df,
        numerical_value.PERCENTAGE_OF_DATE_TO_REMOVE_AS_OUTLIERS,
    )

    care_homes_within_standardised_residual_cutoff_df = create_filtered_job_count_df(
        data_to_filter_df,
        lower_standardised_residual_cutoff,
        upper_standardised_residual_cutoff,
        column_to_filter,
        filtered_col_name,
    )

    care_homes_with_filtered_col_df = join_filtered_col_into_care_home_df(
        care_homes_df, care_homes_within_standardised_residual_cutoff_df
    )

    data_not_relevant_to_filter_df = (
        add_job_counts_without_filtering_to_data_outside_of_this_filter(
            data_not_relevant_to_filter_df, column_to_filter, filtered_col_name
        )
    )

    output_df = combine_dataframes(
        care_homes_with_filtered_col_df, data_not_relevant_to_filter_df
    )

    return output_df


def select_relevant_data(
    input_df: pyspark.sql.DataFrame, column_to_filter: str
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    output_df = input_df.where(F.col(column_name.registration_status) == "Registered")
    output_df = output_df.where(F.col(column_name.carehome) == "Y")
    output_df = output_df.where(F.col(column_name.number_of_beds) > 0)
    output_df = output_df.where(F.col(column_to_filter).isNotNull())
    output_df = output_df.where(F.col(column_to_filter) > 0.0)

    return output_df


def select_data_not_in_subset_df(
    complete_df: pyspark.sql.DataFrame, subset_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    output_df = complete_df.exceptAll(subset_df)

    return output_df


def calculate_jobs_per_bed_ratio(
    input_df: pyspark.sql.DataFrame, column_to_filter: str
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    input_df = input_df.withColumn(
        column_name.jobs_per_bed_ratio,
        F.col(column_to_filter) / F.col(column_name.number_of_beds),
    )
    input_df = round_figures_in_column(input_df, column_name.jobs_per_bed_ratio)

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
    set_bucket_names = {
        0.0: "1-2 beds",
        1.0: "3-4 beds",
        2.0: "5-9 beds",
        3.0: "10-14 beds",
        4.0: "15-19 beds",
        5.0: "20-24 beds",
        6.0: "25-49 beds",
        7.0: "50+ beds",
    }

    input_df = set_banded_boundaries.setHandleInvalid("keep").transform(input_df)

    udf_buckets = F.udf(lambda x: set_bucket_names[x], StringType())

    input_df = input_df.withColumn(
        column_name.number_of_beds_banded,
        udf_buckets(column_name.number_of_beds_banded),
    )

    return input_df


def calculate_average_jobs_per_banded_bed_count(
    input_df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    output_df = input_df.groupBy(F.col(column_name.number_of_beds_banded)).agg(
        F.avg(column_name.jobs_per_bed_ratio).alias(column_name.avg_jobs_per_bed_ratio)
    )
    output_df = round_figures_in_column(output_df, column_name.avg_jobs_per_bed_ratio)

    return output_df


def calculate_standardised_residuals(
    df: pyspark.sql.DataFrame,
    expected_jobs_per_banded_bed_count_df: pyspark.sql.DataFrame,
    column_to_filter: str,
) -> pyspark.sql.DataFrame:

    df = calculate_expected_jobs_based_on_number_of_beds(
        df, expected_jobs_per_banded_bed_count_df
    )
    df = calculate_job_count_residuals(df, column_to_filter)
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
    df = round_figures_in_column(df, column_name.expected_jobs)

    return df


def calculate_job_count_residuals(
    df: pyspark.sql.DataFrame, column_to_filter: str
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    df = df.withColumn(
        column_name.residual,
        F.col(column_to_filter) - F.col(column_name.expected_jobs),
    )
    df = round_figures_in_column(df, column_name.residual)

    return df


def calculate_job_count_standardised_residual(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    df = df.withColumn(
        column_name.standardised_residual,
        F.col(column_name.residual) / F.sqrt(F.col(column_name.expected_jobs)),
    )
    df = round_figures_in_column(df, column_name.standardised_residual)

    return df


def calculate_standardised_residual_cutoffs(
    df: pyspark.sql.DataFrame, percentage_of_data_to_filter_out: float
) -> pyspark.sql.DataFrame:
    standardised_residual = ColNames.standardised_residual

    lower_percentile = percentage_of_data_to_filter_out / 2
    upper_percentile = 1 - (percentage_of_data_to_filter_out / 2)

    standardised_residual_lower_cutoff = calculate_percentile(
        df, standardised_residual, lower_percentile
    )
    standardised_residual_upper_cutoff = calculate_percentile(
        df, standardised_residual, upper_percentile
    )

    return standardised_residual_lower_cutoff, standardised_residual_upper_cutoff


def calculate_percentile(
    df: pyspark.sql.DataFrame, col_name: str, percentile_value: float
) -> DoubleType:
    df = df.agg(
        F.expr("percentile(" + col_name + ", array(" + str(percentile_value) + "))")[
            0
        ].alias("percentile")
    )

    df = round_figures_in_column(df, "percentile")

    percentile = df.collect()[0][0]

    return percentile


def create_filtered_job_count_df(
    df: pyspark.sql.DataFrame,
    lower_standardised_residual_cutoff: DoubleType,
    upper_standardised_residual_cutoff: DoubleType,
    column_to_filter: str,
    filtered_col_name: str,
) -> pyspark.sql.DataFrame:
    column_name = ColNames()

    within_boundary_df = df.filter(
        (F.col(column_name.standardised_residual) > lower_standardised_residual_cutoff)
        & (
            F.col(column_name.standardised_residual)
            < upper_standardised_residual_cutoff
        )
    )

    within_boundary_df = within_boundary_df.withColumn(
        filtered_col_name, F.col(column_to_filter)
    )

    output_df = within_boundary_df.select(
        column_name.locationid, column_name.snapshot_date, filtered_col_name
    )

    return output_df


def join_filtered_col_into_care_home_df(
    df: pyspark.sql.DataFrame, df_with_filtered_column: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    locationid = ColNames.locationid
    snapshot_date = ColNames.snapshot_date

    return df.join(df_with_filtered_column, [locationid, snapshot_date], "left")


def add_job_counts_without_filtering_to_data_outside_of_this_filter(
    df: pyspark.sql.DataFrame,
    column_to_filter: str,
    filtered_col_name: str,
) -> pyspark.sql.DataFrame:

    return df.withColumn(filtered_col_name, F.col(column_to_filter))


def combine_dataframes(
    first_df: pyspark.sql.DataFrame, second_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:

    output_df = first_df.unionByName(second_df)

    return output_df


def round_figures_in_column(
    input_df: pyspark.sql.DataFrame,
    column_to_round: str,
    decimal_places=NumericalValues.DECIMAL_PLACES_TO_ROUND_TO,
) -> pyspark.sql.DataFrame:

    input_df = input_df.withColumn(
        column_to_round, F.round(F.col(column_to_round), decimal_places)
    )

    return input_df
