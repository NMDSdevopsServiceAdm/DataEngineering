from dataclasses import dataclass, fields

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class TempCols:
    """The names of the temporary columns used in this process."""

    days_to_forward_fill: str = "days_to_forward_fill"
    last_known_date: str = "last_known_date"
    last_known_value: str = "last_known_value"
    latest: str = "latest"


# Dictionary defining the minimum size of location and corresponding days to forward fill.
SIZE_BASED_FORWARD_FILL_DAYS = {
    -float("inf"): 250,  # small location, using 250 as proxy for 8 month repetition.
    10: 125,  # medium location, using 125 as proxy for 4 month repetition.
    50: 65,  # large location, using 65 as proxy for 2 month repetition.
}


def forward_fill_latest_known_value(
    df: DataFrame, col_to_forward_fill: str
) -> DataFrame:
    """
    Forward-fills the last known value of a column for each location, using a length of time base on the location size.

    The length of time to forward-fill is determined by the SIZE_BASED_FORWARD_FILL_DAYS dictionary.

    This function has the following steps:
        1. Determine a size-based number of days to forward-fill for each row.
        2. Identify the latest non-null value and its date for each location.
        3. Forward-fill `col_to_forward_fill` for rows where:
            - the column value is null,
            - the import date is after the last known submission date, and
            - the date difference is within the limit defined by `days_to_forward_fill`.
        4. Drop intermediate helper columns.

    Args:
        df (DataFrame): Input DataFrame containing `location_id`, `cqc_location_import_date` and col_to_forward_fill.
        col_to_forward_fill (str): Name of the column whose last known value will be propagated forward.

    Returns:
        DataFrame: Input DataFrame with nulls in `col_to_forward_fill` forward-filled according to the size-based length of time.
    """
    last_known_df = return_last_known_value(df, col_to_forward_fill)
    last_known_df = add_size_based_forward_fill_days(
        last_known_df, TempCols.last_known_value, SIZE_BASED_FORWARD_FILL_DAYS
    )

    df = df.join(last_known_df, on=IndCQC.location_id, how="left")

    forward_fill_df = forward_fill(df, col_to_forward_fill)

    columns_to_drop = [field.name for field in fields(TempCols())]
    forward_fill_df = forward_fill_df.drop(*columns_to_drop)

    return forward_fill_df


def add_size_based_forward_fill_days(
    df: DataFrame, location_size_col: str, size_based_forward_fill_days: dict[int, int]
) -> DataFrame:
    """
    Adds a column to the DataFrame indicating the number of days to forward fill based on location size.

    Args:
        df (DataFrame): Input DataFrame containing `location_size` column.
        location_size_col (str): Column name for determining location size.
        size_based_forward_fill_days (dict[int, int]): Dictionary mapping minimum location sizes to days to forward fill.

    Returns:
        DataFrame: DataFrame with an additional column indicating days to forward fill.
    """
    expr = None

    for min_size, days in sorted(size_based_forward_fill_days.items()):
        condition = F.col(location_size_col) >= min_size
        if expr is None:
            expr = F.when(condition, F.lit(days))
        else:
            expr = F.when(condition, F.lit(days)).otherwise(expr)

    return df.withColumn(TempCols.days_to_forward_fill, expr)


def return_last_known_value(df: DataFrame, col_to_forward_fill: str) -> DataFrame:
    """
    This function gets the last known non-null value for col_to_forward_fill and the date for this value.

    Args:
        df (DataFrame): Input DataFrame with column to forward fill.
        col_to_forward_fill (str): Column for which we need the last known non-null value.

    Returns:
        DataFrame: A DataFrame with last known non-null value and the date when it has the last non-null value.
    """
    df_with_last_known = (
        df.filter(F.col(col_to_forward_fill).isNotNull())
        .groupBy(IndCQC.location_id)
        .agg(
            F.max(
                F.struct(
                    F.col(IndCQC.cqc_location_import_date).alias(
                        TempCols.last_known_date
                    ),
                    F.col(col_to_forward_fill).alias(TempCols.last_known_value),
                )
            ).alias(TempCols.latest)
        )
        .select(
            IndCQC.location_id,
            F.col(f"{TempCols.latest}.{TempCols.last_known_date}").alias(
                TempCols.last_known_date
            ),
            F.col(f"{TempCols.latest}.{TempCols.last_known_value}").alias(
                TempCols.last_known_value
            ),
        )
    )
    return df_with_last_known


def forward_fill(df: DataFrame, col_to_forward_fill: str) -> DataFrame:
    """
    Populates null values in a column based on repeating the last known value for a set number of days.

    Args:
        df (DataFrame): Input DataFrame with last known value and date.
        col_to_forward_fill (str): Column name to apply forward filling.

    Returns:
        DataFrame: Dataframe with forward filled column.
    """
    after_last_known_date = F.col(IndCQC.cqc_location_import_date) > F.col(
        TempCols.last_known_date
    )

    within_days_to_fill = F.datediff(
        F.col(IndCQC.cqc_location_import_date),
        F.col(TempCols.last_known_date),
    ) <= F.col(TempCols.days_to_forward_fill)

    forward_fill_condition = (
        F.col(col_to_forward_fill).isNull()
        & after_last_known_date
        & within_days_to_fill
    )

    df = df.withColumn(
        col_to_forward_fill,
        F.when(forward_fill_condition, F.col(TempCols.last_known_value)).otherwise(
            F.col(col_to_forward_fill)
        ),
    )

    return df
