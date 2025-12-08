from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def forward_fill_latest_known_value(
    df: DataFrame,
    col_to_repeat: str,
    days_to_repeat: int,
) -> DataFrame:
    """
    Copies the latest known value of col_to_repeat forward for the given days_to_repeat through cqc_location_import_date
    per locationid.

    A generalised function which allows a given column to be forward-filled by making the propagation window configurable.
    This avoids hardcoding logic and makes the function suitable for reuse with other datasets (e.g., PIR data) and
    with varying time windows as requirements change. This function has the following steps:
        1. Identify the latest non-null value for `col_to_repeat` per location and that values import date.
        2. Join this information back onto the main dataset.
        3. Forward-fill the column for rows where:
            - the column value is null,
            - the import date is after the last known submission date, and
            - the date difference does not exceed `days_to_repeat`.
        4. Drop intermediate helper columns.

    Args:
        df (DataFrame): Input DataFrame containing `location_id`, `cqc_location_import_date` and col_to_repeat.
        col_to_repeat (str): Name of the column whose last known value will be propagated forward.
        days_to_repeat (int): Maximum number of days after the last known submission for which the
                            value should be forward-filled.

    Returns:
        DataFrame: The input DataFrame with null values in `col_to_repeat` replaced where appropriate.
    """
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

    df2 = df.join(last_known, on=IndCQC.location_id, how="left")

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

    df2 = df2.drop(last_known_date, last_known_value).orderBy(
        IndCQC.location_id, IndCQC.cqc_location_import_date
    )

    return df2
