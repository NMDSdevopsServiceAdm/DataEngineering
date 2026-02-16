from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


# converted to polars -> projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value.forward_fill_latest_known_value
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
    df_with_last_known = return_last_known_value(df, col_to_repeat)

    df_with_last_known = df.join(df_with_last_known, on=IndCQC.location_id, how="left")

    df_with_forward_fill = forward_fill(
        df_with_last_known, col_to_repeat, days_to_repeat
    )

    return df_with_forward_fill


# converted to polars -> projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value.return_last_known_value
def return_last_known_value(
    df: DataFrame,
    col_to_repeat: str,
) -> DataFrame:
    """
    This function gets the last known non-null value for col_to_repeat and the date for this value

    Args:
        df (DataFrame): Input DataFrame with column to repeat.
        col_to_repeat (str): Column for which we need the last known non-null value.

    Returns:
        DataFrame: A DataFrame with last known non-null value and the date when it has the last non-null value.
    """
    last_known_date = "last_known_date"
    last_known_value = "last_known_value"
    df_with_last_known = (
        df.filter(F.col(col_to_repeat).isNotNull())
        .groupBy(IndCQC.location_id)
        .agg(
            F.max(
                F.struct(
                    F.col(IndCQC.cqc_location_import_date).alias(last_known_date),
                    F.col(col_to_repeat).alias(last_known_value),
                )
            ).alias("latest")
        )
        .select(
            IndCQC.location_id,
            F.col(f"latest.{last_known_date}").alias(last_known_date),
            F.col(f"latest.{last_known_value}").alias(last_known_value),
        )
    )
    return df_with_last_known


# converted to polars -> projects._03_independent_cqc._02_clean.fargate.utils.forward_fill_latest_known_value.forward_fill
def forward_fill(
    df_with_last_known: DataFrame,
    col_to_repeat: str,
    days_to_repeat: int,
    last_known_date_col: str = "last_known_date",
    last_known_value_col: str = "last_known_value",
) -> DataFrame:
    """
    This function forward fills the column to repeat with the last known value for the number of days we want it
    to repeat. It checks if the import date and last known date is less than days_to_repeat, if it is it fills
    those with last known value calculated in previous function.

    Args:
        df_with_last_known (DataFrame): Input DataFrame with last know value and date.
        col_to_repeat (str): Column name which needs forward filling.
        days_to_repeat (int): Number of days a value needs to be repeated.
        last_known_date_col (str): Last Known Date column name defaulted to last_known_date.
        last_known_value_col (str): Last Known Value column defaulted to last_known_value

    Returns:
        DataFrame: Dataframe with forward filled column.
    """
    df_with_forward_fill = df_with_last_known.withColumn(
        col_to_repeat,
        F.when(
            (F.col(col_to_repeat).isNull())
            & (F.col(IndCQC.cqc_location_import_date) > F.col(last_known_date_col))
            & (
                F.datediff(
                    F.col(IndCQC.cqc_location_import_date), F.col(last_known_date_col)
                )
                <= days_to_repeat
            ),
            F.col(last_known_value_col),
        ).otherwise(F.col(col_to_repeat)),
    )

    return df_with_forward_fill.drop(last_known_date_col, last_known_value_col)
