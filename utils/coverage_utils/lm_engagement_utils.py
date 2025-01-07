from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_values.categorical_column_values import (
    InAscwds,
)


def add_columns_for_locality_manager_dashboard(df: DataFrame) -> DataFrame:
    """
    Adds the columns required for the locality manager dashboard.

    Args:
        df (DataFrame): A dataframe with coverage data

    Returns:
        DataFrame: The same dataframe with additional columns containing data for the locality manager dashboard
    """
    w, agg_w, ytd_w = create_windows_for_lm_engagement_calculations()
    df = calculate_la_coverage_monthly(df, agg_w)
    df = calculate_coverage_monthly_change(df, w)
    df = calculate_locations_monthly_change(df, w, agg_w)
    df = calculate_new_registrations(df, agg_w, ytd_w)
    return df


def create_windows_for_lm_engagement_calculations() -> tuple:
    """
    Creates the windows required for the locality manager dashboard.

    Returns:
        tuple: A tuple of windows required for the locality manager dashboard.
    """
    w = Window.partitionBy(CQCLClean.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )
    agg_w = (
        Window.partitionBy(CQCLClean.current_cssr, CQCLClean.cqc_location_import_date)
        .orderBy(CQCLClean.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    ytd_w = (
        Window.partitionBy(CQCLClean.current_cssr, Keys.year)
        .orderBy(CQCLClean.cqc_location_import_date)
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return w, agg_w, ytd_w


def calculate_la_coverage_monthly(df: DataFrame, agg_w: Window) -> DataFrame:
    """
    Adds a column with the monthly coverage for the local authority.

    Args:
        df (DataFrame): A dataframe with coverage data
        agg_w (Window): A window which aggregates the data over local authorities and import dates

    Returns:
        DataFrame: The same dataframe with the monthly coverage column added.
    """
    df = df.withColumn(
        CoverageColumns.la_monthly_locations_count,
        F.count(F.col(CQCLClean.location_id)).over(agg_w),
    )
    df = df.withColumn(
        CoverageColumns.la_monthly_locations_in_ascwds_count,
        F.sum(F.col(CoverageColumns.in_ascwds)).over(agg_w),
    )
    df = df.withColumn(
        CoverageColumns.la_monthly_coverage,
        F.col(CoverageColumns.la_monthly_locations_in_ascwds_count)
        / F.col(CoverageColumns.la_monthly_locations_count),
    )
    df = df.drop(
        CoverageColumns.la_monthly_locations_count,
        CoverageColumns.la_monthly_locations_in_ascwds_count,
    )
    return df


def calculate_coverage_monthly_change(df: DataFrame, w: Window) -> DataFrame:
    """
    Adds a column with the coverage monthly change for the local authority.

    Args:
        df (DataFrame): A dataframe with coverage data
        w (Window): A window which groups the data by location

    Returns:
        DataFrame: The same dataframe with the coverage monthly change column added.
    """
    df = df.withColumn(
        CoverageColumns.la_monthly_coverage_last_month,
        F.lag(CoverageColumns.la_monthly_coverage).over(w),
    )
    df = df.withColumn(
        CoverageColumns.coverage_monthly_change,
        F.col(CoverageColumns.la_monthly_coverage)
        - F.col(CoverageColumns.la_monthly_coverage_last_month),
    )
    df = df.drop(
        CoverageColumns.la_monthly_coverage_last_month,
    )
    return df


def calculate_locations_monthly_change(
    df: DataFrame, w: Window, agg_w: Window
) -> DataFrame:
    """
    Adds a column with the locations monthly change for the local authority and in ascwds last month column for the location.

    Args:
        df (DataFrame): A dataframe with coverage data
        w (Window): A window which groups the data by location
        agg_w (Window): A window which aggregates the data over local authorities and import dates

    Returns:
        DataFrame: The same dataframe with the locations monthly change column and in ascwds last month column added.
    """
    df = df.withColumn(
        CoverageColumns.in_ascwds_last_month,
        F.lag(CoverageColumns.in_ascwds).over(w),
    )
    df = df.na.fill(InAscwds.not_in_ascwds, CoverageColumns.in_ascwds_last_month)
    df = df.withColumn(
        CoverageColumns.in_ascwds_change,
        F.col(CoverageColumns.in_ascwds) - F.col(CoverageColumns.in_ascwds_last_month),
    )
    df = df.withColumn(
        CoverageColumns.locations_monthly_change,
        F.sum(CoverageColumns.in_ascwds_change).over(agg_w),
    )
    df = df.drop(
        CoverageColumns.in_ascwds_change,
    )
    return df


def calculate_new_registrations(
    df: DataFrame, agg_w: Window, ytd_w: Window
) -> DataFrame:
    """
    Adds columns with the monthly new registrations and the number of new registrations for the year to date for the local authority.

    Adds columns with the monthly new registrations and the number of new registrations for the year to date for the local
    authority. The in ascwds last month column is also dropped after being used for these calculations. NB: new registrations
    do not take into account new de-registrations in their calculation.

    Args:
        df (DataFrame): A dataframe with coverage data
        agg_w (Window): A window which aggregates the data over local authorities and import dates
        ytd_w (Window): A window which aggregates the year to date for local authorities and years

    Returns:
        DataFrame: The same dataframe with with the monthly new registrations and the number of new registrations for the year to date columns added.
    """
    df = df.withColumn(
        CoverageColumns.new_registration,
        F.when(
            (
                (F.col(CoverageColumns.in_ascwds) == 1)
                & (F.col(CoverageColumns.in_ascwds_last_month) == 0)
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    df = df.withColumn(
        CoverageColumns.new_registrations_monthly,
        F.sum(CoverageColumns.new_registration).over(agg_w),
    )
    df = df.withColumn(
        CoverageColumns.new_registrations_ytd,
        F.sum(CoverageColumns.new_registration).over(ytd_w),
    )
    df = df.drop(
        CoverageColumns.new_registration,
        CoverageColumns.in_ascwds_last_month,
    )
    return df
