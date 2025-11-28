from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projects._03_independent_cqc.utils.utils.utils import get_selected_value
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def duplicate_latest_known_value_into_following_two_rows(df: DataFrame) -> DataFrame:
    """
    Duplicates the last known value in ascwds_filled_posts_dedup_clean into the following two rows.

    Args:
        df (DataFrame): A DataFrame with ascwds_filled_posts_dedup_clean column and cqc_location_import_date.

    Returns:
        DataFrame: The input DataFrame with additional values in ascwds_filled_posts_dedup_clean.
    """
    window_spec = Window.partitionBy(IndCQC.location_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    window_spec_whole_partition = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    df = df.withColumn(
        "import_date_of_last_known_value",
        F.max(
            F.when(
                F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNotNull(),
                F.col(IndCQC.cqc_location_import_date),
            )
        ).over(window_spec_whole_partition),
    )

    df = df.withColumn(
        "lastest_value",
        F.when(
            F.col(IndCQC.cqc_location_import_date)
            == F.col("import_date_of_last_known_value"),
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
        ),
    )

    df = df.withColumn(
        "lastest_value_copied_forwards",
        F.coalesce(
            F.col("lastest_value"),
            F.lag(F.col("lastest_value"), 1).over(window_spec),
            F.lag(F.col("lastest_value"), 2).over(window_spec),
        ),
    )

    # TODO update the filtering rule ?

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.coalesce(
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
            F.col("lastest_value_copied_forwards"),
        ),
    )

    return df.drop(
        "import_date_of_last_known_value",
        "lastest_value",
        "lastest_value_copied_forwards",
    )
