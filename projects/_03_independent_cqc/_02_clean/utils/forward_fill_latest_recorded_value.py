from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def duplicate_latest_known_ascwds_value_into_following_two_import_dates(
    df: DataFrame,
) -> DataFrame:
    """
    Duplicates the latest known ascwds_filled_posts_dedup_clean value into the following two import dates.

    Analysis of Capacity Tracker data has shown that locations have the same filled posts value for an average of
    3 months. Therefore, the latest ASC-WDS submission will be copied into the following two import_dates, to create
    a period of 3 months with the same value. In the estimates pipeline, import_dates are always 1 month apart at this time.

    Args:
        df (DataFrame): A DataFrame with ascwds_filled_posts_dedup_clean column and cqc_location_import_date.

    Returns:
        DataFrame: The input DataFrame with additional values in ascwds_filled_posts_dedup_clean.
    """
    import_date_of_last_known_value = "import_date_of_last_known_value"
    window_spec = Window.partitionBy(IndCQC.location_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    window_spec_whole_partition = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    df = df.withColumn(
        import_date_of_last_known_value,
        F.max(
            F.when(
                F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNotNull(),
                F.col(IndCQC.cqc_location_import_date),
            )
        ).over(window_spec_whole_partition),
    )

    lastest_value = "lastest_value"
    df = df.withColumn(
        lastest_value,
        F.when(
            F.col(IndCQC.cqc_location_import_date)
            == F.col(import_date_of_last_known_value),
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
        ),
    )

    lastest_value_copied_forwards = "lastest_value_copied_forwards"
    df = df.withColumn(
        lastest_value_copied_forwards,
        F.coalesce(
            F.col(lastest_value),
            F.lag(F.col(lastest_value), 1).over(window_spec),
            F.lag(F.col(lastest_value), 2).over(window_spec),
        ),
    )
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.coalesce(
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
            F.col(lastest_value_copied_forwards),
        ),
    )

    return df.drop(
        import_date_of_last_known_value,
        lastest_value,
        lastest_value_copied_forwards,
    )


def repeat_last_known_value(
    df: DataFrame,
    col_to_repeat: str,
    days_to_repeat: int,
) -> DataFrame:
    """
    Propagates the most recent known value of a specified column forward for a configurable
    number of days after the last non-null submission for each location.

    A generalised function which allows any column to be forward-filled by making the propagation window configurable.
    This avoids hardcoding logic and makes the function suitable for reuse with other datasets (e.g., PIR data) and
    with varying time windows as requirements change. This function follows following steps:
        1. Identify the latest non-null value for `col_to_repeat` per location, including the date it
        was observed.
        2. Join this information back onto the main dataset.
        3. Forward-fill the column for rows where:
            - the column value is null,
            - the import date is after the last known submission date, and
            - the date difference does not exceed `days_to_repeat`.
        4. Drop intermediate helper columns.

    Args:
        df (DataFrame): Input dataset containing `location_id`, `cqc_location_import_date`,
                        and the column to be forward-filled.
        col_to_repeat (str): Name of the column whose last known value should be propagated forward.
        days_to_repeat (int): Maximum number of days after the last known submission for which the
                            value should be forward-filled.

    Returns:
        DataFrame: The input DataFrame with null values in `col_to_repeat` replaced where appropriate.
    """

    # --- 1. Get latest non-null value per location ---
    last_known_date = "last_known_date"
    last_known_value = "last_known_value"
    last_known = (
        df.filter(F.col(col_to_repeat).isNotNull())
        .select(
            IndCQC.location_id,
            F.col(IndCQC.cqc_location_import_date).alias(last_known_date),
            F.col(col_to_repeat).alias(last_known_value),
        )
        .groupBy(IndCQC.location_id)
        .agg(F.max(F.struct(last_known_date, last_known_value)).alias("struct_col"))
        .select(
            IndCQC.location_id,
            F.col(f"struct_col.{last_known_date}"),
            F.col(f"struct_col.{last_known_value}"),
        )
    )

    # --- 2. Join back to full df ---
    df2 = df.join(last_known, on=IndCQC.location_id, how="left")

    # --- 3. Apply forward-fill rule ---
    df2 = df2.withColumn(
        col_to_repeat,
        F.when(
            (F.col(col_to_repeat).isNull())
            & (F.col(IndCQC.cqc_location_import_date) > F.col(last_known_date))
            & (
                F.datediff(
                    F.col(IndCQC.cqc_location_import_date), F.col(last_known_date)
                )
                <= days_to_repeat
            ),
            F.col(last_known_value),
        ).otherwise(F.col(col_to_repeat)),
    )

    # --- 4. Drop temporary columns ---
    df2 = df2.drop(last_known_date, last_known_value).orderBy(
        IndCQC.location_id, IndCQC.cqc_location_import_date
    )

    return df2
