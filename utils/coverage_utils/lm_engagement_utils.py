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
    df = calculate_la_coverage_monthly(df, agg_w)
    df = calculate_coverage_monthly_change(df, w)
    df = calculate_locations_monthly_change(df, w, agg_w)
    df = calculate_new_registrations(df, agg_w, ytd_w)
    return df


def calculate_la_coverage_monthly(df: DataFrame, w: Window) -> DataFrame:
    df = df.withColumn(
        CoverageColumns.la_monthly_locations_count,
        F.count(F.col(CQCLClean.location_id)).over(w),
    )
    df = df.withColumn(
        CoverageColumns.la_monthly_locations_in_ascwds_count,
        F.sum(F.col(CoverageColumns.in_ascwds)).over(w),
    )
    df = df.withColumn(
        CoverageColumns.la_monthly_coverage,
        F.col(CoverageColumns.la_monthly_locations_in_ascwds_count)
        / F.col(CoverageColumns.la_monthly_locations_count),
    )
    df.show()
    df = df.drop(
        CoverageColumns.la_monthly_locations_count,
        CoverageColumns.la_monthly_locations_in_ascwds_count,
    )
    return df


def calculate_coverage_monthly_change(df: DataFrame, w: Window) -> DataFrame:
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
    df.show()
    df = df.drop(
        CoverageColumns.new_registration,
        CoverageColumns.in_ascwds_last_month,
    )
    return df
