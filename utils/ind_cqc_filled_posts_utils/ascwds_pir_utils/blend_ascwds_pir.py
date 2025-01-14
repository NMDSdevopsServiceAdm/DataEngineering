from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    get_selected_value,
)


def blend_pir_and_ascwds_when_ascwds_out_of_date(df: DataFrame) -> DataFrame:
    df = create_repeated_ascwds_clean_column(df)
    # TODO: create pir dedup modelled column for comparison
    df = create_people_directly_employed_dedup_modelled_column(df)
    # TODO: Abstract get selected value functions into function to create last submission dates
    df = create_last_submission_columns()
    # TODO: for rows where pir is more than 2 years later than asc and gap in value is greater than +/- 100 and +/- 50% and pir filled posts is not null, add pir filled posts into ascwds clean column
    df = merge_people_directly_employed_modelled_into_ascwds_clean_column(df)
    # TODO: drop unwanted columns
    df = drop_unwanted_columns(df)
    return df


def create_repeated_ascwds_clean_column(df: DataFrame) -> DataFrame:
    w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean_repeated,
        F.last(IndCQC.ascwds_filled_posts_dedup_clean, ignorenulls=True).over(w),
    )
    return df


def create_people_directly_employed_dedup_modelled_column(df: DataFrame) -> DataFrame:
    return df


def create_last_submission_columns(df: DataFrame) -> DataFrame:
    w = w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = get_selected_value(
        df,
        w,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.cqc_location_import_date,
        "last_ascwds_submission",
        "last",
    )
    df = get_selected_value(
        df,
        w,
        IndCQC.people_directly_employed_dedup,  # use modelled column instead
        IndCQC.cqc_location_import_date,
        "last_pir_submission",
        "last",
    )

    return df


def merge_people_directly_employed_modelled_into_ascwds_clean_column(
    df: DataFrame,
) -> DataFrame:
    return df


def drop_unwanted_columns(df: DataFrame) -> DataFrame:
    return df
