from dataclasses import fields

from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
    NullPeopleDirectlyEmployedTemporaryColumns as TempCol,
)
from projects.utils.utils.utils import calculate_new_column

PROPORTION_OF_DATA_TO_REMOVE: float = (
    0.01  # filter out the highest 1% of extreme values
)


def null_people_directly_employed_outliers(df: DataFrame) -> DataFrame:
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

    df = null_outliers(df, proportion_of_data_to_filter=PROPORTION_OF_DATA_TO_REMOVE)

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

    one_submission: int = 1
    large_location_identifier: int = 100

    df = df.withColumn(
        TempCol.submission_count,
        F.count(PIRCleanCols.pir_people_directly_employed_cleaned).over(w),
    )

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.when(
            (
                F.col(PIRCleanCols.pir_people_directly_employed_cleaned)
                >= large_location_identifier
            )
            & (F.col(TempCol.submission_count) == one_submission),
            F.lit(None),
        ).otherwise(F.col(PIRCleanCols.pir_people_directly_employed_cleaned)),
    )
    df = df.drop(TempCol.submission_count)

    return df


def null_outliers(df: DataFrame, proportion_of_data_to_filter: float) -> DataFrame:
    """
    Outliers detection pipeline to flag outliers and clean directly employed staff values.

    Args:
        df (DataFrame): Raw input with 'location_id' and 'staff'.
        proportion_of_data_to_filter (float): Percentile threshold for flagging.

    Returns:
        DataFrame: DataFrame with 'staff_cleaned' and removal flags.
    """
    df_dispersion = compute_dispersion_stats(df)
    df_mad = compute_median_absolute_deviation_stats(df)

    df_flags = flag_outliers(df_dispersion, df_mad, proportion_of_data_to_filter)
    cleaned_df = apply_removal_flag(df, df_flags)

    columns_to_drop = [field.name for field in fields(TempCol())]
    cleaned_df = cleaned_df.drop(*columns_to_drop)

    return cleaned_df


def compute_dispersion_stats(df: DataFrame) -> DataFrame:
    """
    Computes the dispersion ratio for each workplace.

    The dispersion ratio is defined as:
        (max(submission) - min(submission)) / mean(submission)

    Args:
        df (DataFrame): Input DataFrame with columns 'location_id' and 'pir_people_directly_employed_cleaned'.

    Returns:
        DataFrame: Contains 'location_id' and the dispersion_ratio.
    """
    df_agg = df.groupBy(PIRCleanCols.location_id).agg(
        F.max(PIRCleanCols.pir_people_directly_employed_cleaned).alias(
            TempCol.max_people_employed
        ),
        F.min(PIRCleanCols.pir_people_directly_employed_cleaned).alias(
            TempCol.min_people_employed
        ),
        F.mean(PIRCleanCols.pir_people_directly_employed_cleaned).alias(
            TempCol.mean_people_employed
        ),
    )
    df_agg = df_agg.withColumn(
        TempCol.dispersion_ratio,
        (F.col(TempCol.max_people_employed) - F.col(TempCol.min_people_employed))
        / F.col(TempCol.mean_people_employed),
    )

    return df_agg


def compute_median_absolute_deviation_stats(df: DataFrame) -> DataFrame:
    """
    Computes the Median Absolute Deviation (MAD) per workplace.

    Steps:
        1. Compute the median staff count per workplace.
        2. Compute absolute deviations from the median.
        3. Compute the median of those deviations (MAD) per workplace.

    Args:
        df (DataFrame): Input DataFrame with columns 'location_id' and 'staff'.

    Returns:
        DataFrame: Contains distinct 'location_id' and 'median_absolute_deviation'.
    """
    df = compute_median(
        df,
        PIRCleanCols.pir_people_directly_employed_cleaned,
        TempCol.median_people_employed,
    )
    df = calculate_new_column(
        df,
        TempCol.absolute_deviation,
        PIRCleanCols.pir_people_directly_employed_cleaned,
        "absolute difference",
        TempCol.median_people_employed,
    )
    df = compute_median(
        df,
        TempCol.absolute_deviation,
        TempCol.median_absolute_deviation,
    )

    mad_df = df.select(
        PIRCleanCols.location_id, TempCol.median_absolute_deviation
    ).distinct()

    return mad_df


def compute_median(df: DataFrame, col_to_analyse: str, new_col_name: str) -> DataFrame:
    """
    Computes the median of a specified column grouped by 'location_id' and stores it in a new named column.

    Args:
        df (DataFrame): Input DataFrame with 'location_id' and the column to analyse.
        col_to_analyse (str): The column for which to compute the median.
        new_col_name (str): The name of the new column to store the median values.

    Returns:
        DataFrame: The original DataFrame with an additional column containing the median values.
    """
    median: float = 0.5

    median_df = df.groupBy(PIRCleanCols.location_id).agg(
        F.expr(f"percentile({col_to_analyse}, array({median}))")[0].alias(new_col_name),
    )

    df = df.join(median_df, PIRCleanCols.location_id, "left")

    return df


def flag_outliers(
    df_dispersion: DataFrame, df_mad: DataFrame, proportion_of_data_to_filter: float
) -> DataFrame:
    """
    Flags workplaces in the top percentile of dispersion ratio or MAD value.

    Args:
        df_dispersion (DataFrame): DataFrame with 'location_id' and 'dispersion_ratio'.
        df_mad (DataFrame): DataFrame with 'location_id' and 'mad_value'.
        proportion_of_data_to_filter (float): Proportion of data to remove as outliers.

    Returns:
        DataFrame: Contains 'location_id', 'dispersion_flag', and 'mad_flag'.
    """
    proportion_of_data_to_keep = 1 - proportion_of_data_to_filter

    df_joined = df_dispersion.join(df_mad, on=PIRCleanCols.location_id)

    disp_threshold = df_joined.approxQuantile(
        TempCol.dispersion_ratio, [proportion_of_data_to_keep], 0.01
    )[0]
    mad_threshold = df_joined.approxQuantile(
        TempCol.median_absolute_deviation, [proportion_of_data_to_keep], 0.01
    )[0]

    df_joined = df_joined.withColumn(
        TempCol.dispersion_outlier_flag,
        F.col(TempCol.dispersion_ratio) > disp_threshold,
    )
    df_joined = df_joined.withColumn(
        TempCol.median_absolute_deviation_flag,
        F.col(TempCol.median_absolute_deviation) > mad_threshold,
    )

    df_joined = df_joined.select(
        PIRCleanCols.location_id,
        TempCol.dispersion_outlier_flag,
        TempCol.median_absolute_deviation_flag,
    )
    return df_joined


def apply_removal_flag(
    df_to_clean: DataFrame, df_with_outlier_flags: DataFrame
) -> DataFrame:
    """
    Null all workplace values in pir_people_directly_employed_cleaned if either outlier flag are to True.

    Args:
        df_to_clean (DataFrame): Original DataFrame with 'location_id' and 'pir_people_directly_employed_cleaned'.
        df_with_outlier_flags (DataFrame): DataFrame with 'location_id' and the outlier flags.

    Returns:
        DataFrame: Original DataFrame with 'remove_flag' and 'staff_cleaned' columns.
    """
    df_merged = df_to_clean.join(
        df_with_outlier_flags, on=PIRCleanCols.location_id, how="left"
    )
    df_merged = df_merged.withColumn(
        TempCol.outlier_flag,
        F.col(TempCol.dispersion_outlier_flag)
        | F.col(TempCol.median_absolute_deviation_flag),
    )
    df_merged = df_merged.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.when(F.col(TempCol.outlier_flag), None).otherwise(
            F.col(PIRCleanCols.pir_people_directly_employed_cleaned)
        ),
    )

    return df_merged
