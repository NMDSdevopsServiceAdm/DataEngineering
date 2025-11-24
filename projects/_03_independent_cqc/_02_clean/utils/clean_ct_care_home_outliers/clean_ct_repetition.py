from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import projects._03_independent_cqc.utils.utils.utils as utils
from projects._03_independent_cqc._02_clean.jobs.clean_ind_cqc_filled_posts import (
    create_column_with_repeated_values_removed,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

REPETITION_LIMIT = 365


def null_values_after_consecutive_repetition(
    df: DataFrame, column_to_clean: str
) -> DataFrame:
    """
    Adds a new column in which values are allowed to consecutively repeat for 365 days but then replaced by null from
    366 days onwards.

    This function:
        1. Adds a new column with deduplicated column_to_clean values.
        2. Adds a new column with the import_date of the first time a consecutive repeated value is submitted.
        3. Adds a new column with the days between first consecutive repeat and import_date.
        4. Nulls values when the days between dates is above REPETITION_LIMIT.

    Args:
        df(DataFrame): A dataframe with consecutive import dates.
        column_to_clean(str): The column with repeated values.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """

    df = create_column_with_repeated_values_removed(
        df, column_to_clean, f"{column_to_clean}_deduplicated"
    )

    window_spec = Window.partitionBy(IndCQC.location_id).orderBy(
        IndCQC.cqc_location_import_date
    )
    window_spec_backwards = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df = utils.get_selected_value(
        df,
        window_spec_backwards,
        f"{column_to_clean}_deduplicated",
        IndCQC.cqc_location_import_date,
        IndCQC.previous_submission_import_date,
        "last",
    )

    df = df.withColumn(
        IndCQC.days_since_previous_submission,
        F.date_diff(
            IndCQC.cqc_location_import_date, IndCQC.previous_submission_import_date
        ),
    )

    df = df.withColumn(
        column_to_clean,
        F.when(
            F.col(IndCQC.days_since_previous_submission) <= REPETITION_LIMIT,
            F.col(column_to_clean),
        ).otherwise(None),
    )

    df = df.drop(
        f"{column_to_clean}_deduplicated",
        IndCQC.previous_submission_import_date,
        IndCQC.days_since_previous_submission,
    )

    return df
