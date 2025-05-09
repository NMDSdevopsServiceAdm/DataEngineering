from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
)

PROPORTION_OF_DATA_TO_REMOVE: float = (
    0.01  # filter out the highest 1% of extreme values
)


def clean_people_directly_employed_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of the'pir_people_directly_employed' column.

    This function step duplicates 'pir_people_directly_employed' as 'pir_people_directly_employed_cleaned'
    and then runs various functions designed to remove outliers.

    Args:
        df (DataFrame): A dataframe containing 'pir_people_directly_employed'.

    Returns:
        DataFrame: A dataframe containing 'pir_people_directly_employed_cleaned'.
    """
    print("Cleaning pir_people_directly_employed...")

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.col(PIRCols.pir_people_directly_employed),
    )

    df = null_large_single_submission_locations(df)

    # TODO remove locations with large differences (top 1% of (max-min)/avg?)

    return df


def null_large_single_submission_locations(df: DataFrame) -> DataFrame:
    """
    Excludes CQC PIR data for locations who submitted a large employee figure and only submitted once.

    Large locations can have a big impact in localised data. In order to help identify data quality
    issues, we only want to keep data for larger locations who have submitted more than once. This
    is so we have multiple submissions to verify that the location are consistently large as opposed
    to a data input error.

    Args:
        df (DataFrame): The input DataFrame containing pir_people_directly_employed_cleaned.

    Returns:
        DataFrame: The input DataFrame with large, single submission values removed.
    """
    w = Window.partitionBy(PIRCleanCols.location_id)

    two_submissions: int = 2
    submission_count: str = "submission_count"
    max_people_employed: str = "max_value"
    large_location_identifier: int = 100

    df = df.withColumn(
        submission_count,
        F.count(PIRCleanCols.pir_people_directly_employed_cleaned).over(w),
    )
    df = df.withColumn(
        max_people_employed,
        F.max(PIRCleanCols.pir_people_directly_employed_cleaned).over(w),
    )

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.when(
            (F.col(max_people_employed) >= large_location_identifier)
            & (F.col(submission_count) < two_submissions),
            F.lit(None),
        ).otherwise(F.col(PIRCleanCols.pir_people_directly_employed_cleaned)),
    )
    df = df.drop(max_people_employed, submission_count)

    return df


def null_outliers(df: DataFrame, proportion_of_data_to_filter: float) -> DataFrame:
    """
    Complete pipeline to flag outliers and clean staff values.

    Args:
        df (DataFrame): Raw input with 'location_id' and 'staff'.
        proportion_of_data_to_filter (float): Percentile threshold for flagging.

    Returns:
        DataFrame: DataFrame with 'staff_cleaned' and removal flags.
    """
    df_disp = compute_dispersion_stats(df)
    df_mad = compute_mad_stats(df)
    df_flags = flag_outliers(df_disp, df_mad, proportion_of_data_to_filter)
    return apply_removal_flag(df, df_flags)


def compute_dispersion_stats(df: DataFrame) -> DataFrame:
    """
    Computes the dispersion ratio for each workplace.

    The dispersion ratio is defined as:
        (max(staff) - min(staff)) / mean(staff)

    Args:
        df (DataFrame): Input DataFrame with columns 'location_id' and 'staff'.

    Returns:
        DataFrame: Contains 'location_id', 'max_staff', 'min_staff', 'mean_staff', and 'dispersion_ratio'.
    """
    df_agg = df.groupBy("location_id").agg(
        F.max("staff").alias("max_staff"),
        F.min("staff").alias("min_staff"),
        F.mean("staff").alias("mean_staff"),
    )

    df_agg = df_agg.withColumn(
        "dispersion_ratio",
        (F.col("max_staff") - F.col("min_staff")) / F.col("mean_staff"),
    )

    return df_agg


def compute_mad_stats(df: DataFrame) -> DataFrame:
    """
    Computes the Median Absolute Deviation (MAD) per workplace.

    Steps:
        1. Compute the median staff count per workplace.
        2. Compute absolute deviations from the median.
        3. Compute the median of those deviations (MAD) per workplace.

    Args:
        df (DataFrame): Input DataFrame with columns 'location_id' and 'staff'.

    Returns:
        DataFrame: Contains distinct 'location_id' and 'mad_value'.
    """
    w = Window.partitionBy("location_id")

    df = df.withColumn("staff_median", F.expr("percentile_approx(staff, 0.5)").over(w))

    df = df.withColumn("abs_dev", F.abs(F.col("staff") - F.col("staff_median")))

    df = df.withColumn("mad_value", F.expr("percentile_approx(abs_dev, 0.5)").over(w))

    df_mad = df.select("location_id", "mad_value").distinct()

    return df_mad


def flag_outliers(
    df_dispersion: DataFrame, df_mad: DataFrame, cutoff: float
) -> DataFrame:
    """
    Flags workplaces in the top percentile of dispersion ratio or MAD value.

    Args:
        df_dispersion (DataFrame): DataFrame with 'location_id' and 'dispersion_ratio'.
        df_mad (DataFrame): DataFrame with 'location_id' and 'mad_value'.
        cutoff (float): Percentile threshold.

    Returns:
        DataFrame: Contains 'location_id', 'dispersion_flag', and 'mad_flag'.
    """
    df_joined = df_dispersion.join(df_mad, on="location_id")

    disp_threshold = df_joined.approxQuantile("dispersion_ratio", [cutoff], 0.01)[0]
    mad_threshold = df_joined.approxQuantile("mad_value", [cutoff], 0.01)[0]

    df_joined = df_joined.withColumn(
        "dispersion_flag", F.col("dispersion_ratio") > disp_threshold
    )

    df_joined = df_joined.withColumn("mad_flag", F.col("mad_value") > mad_threshold)

    return df_joined.select("location_id", "dispersion_flag", "mad_flag")


def apply_removal_flag(df_raw: DataFrame, df_flags: DataFrame) -> DataFrame:
    """
    Applies removal flags to the raw staff data and returns a cleaned column.

    If either MAD or dispersion flag is set to True for a workplace,
    its staff values are nullified.

    Args:
        df_raw (DataFrame): Raw DataFrame with 'location_id' and 'staff'.
        df_flags (DataFrame): DataFrame with 'location_id', 'dispersion_flag', and 'mad_flag'.

    Returns:
        DataFrame: Original DataFrame with 'remove_flag' and 'staff_cleaned' columns.
    """
    df_merged = df_raw.join(df_flags, on="location_id", how="left")

    df_merged = df_merged.withColumn(
        "remove_flag", F.col("dispersion_flag") | F.col("mad_flag")
    )

    df_merged = df_merged.withColumn(
        "staff_cleaned", F.when(F.col("remove_flag"), None).otherwise(F.col("staff"))
    )

    return df_merged
