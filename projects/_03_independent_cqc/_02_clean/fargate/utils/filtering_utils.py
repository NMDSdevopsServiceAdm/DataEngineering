from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_filtering_rule_column(
    df: DataFrame,
    filter_rule_col_name: str,
    col_to_filter: str,
    populated_rule: str,
    missing_rule: str,
) -> DataFrame:
    """
    Adds a column which flags if data is present or missing.

    This function adds a new column which identifies if the `col_to_filter` is "populated" or "missing data".

    Args:
        df (DataFrame): A dataframe containing the `col_to_filter` before any filters have been applied to the column.
        filter_rule_col_name (str): The name of the new filtering rule column to be created.
        col_to_filter (str): The name of the column to check for nulls.
        populated_rule (str): The name to assign when data is present.
        missing_rule (str): The name to assign when data is null.

    Returns:
        DataFrame: A dataframe with an additional column that states whether data is present or missing before filters are applied.
    """
    df = df.withColumn(
        filter_rule_col_name,
        F.when(F.col(col_to_filter).isNotNull(), F.lit(populated_rule)).otherwise(
            F.lit(missing_rule)
        ),
    )
    return df


def update_filtering_rule(
    df: DataFrame,
    filter_rule_col_name: str,
    raw_col_name: str,
    clean_col_name: str,
    populated_rule: str,
    new_rule_name: str,
    winsorized_rule: str = None,
) -> DataFrame:
    """
    Updates the text in the filtering rule column to reflect the change.

    This function updates the filtering rule in 2 cases:
    1) where the rule is listed as "populated" but the cleaned data has been nulled or changed from the original value.
    2) if a winsorized process has already occured, where the rule is listed as "winsorized" but the cleaned data has been nulled.

    Args:
        df (DataFrame): A dataframe containing the raw column, cleaned column and filtering rule column.
        filter_rule_col_name (str): The name of the new filtering rule column.
        raw_col_name (str): The name of the original column with values.
        clean_col_name (str): The name of the cleaned column with values.
        populated_rule (str): The rule name when original data is being used.
        new_rule_name (str): The name of the new rule to add.
        winsorized_rule (str, optional): The rule name assigned if data has been winsorized (capped). Defaults to None.

    Returns:
        DataFrame: A dataframe with the filtering rule column updated.
    """
    df = df.withColumn(
        filter_rule_col_name,
        F.when(
            (
                (
                    F.col(clean_col_name).isNull()
                    | (F.col(clean_col_name) != F.col(raw_col_name))
                )
                & (F.col(filter_rule_col_name) == populated_rule)
            ),
            F.lit(new_rule_name),
        ).otherwise(F.col(filter_rule_col_name)),
    )

    if winsorized_rule:
        df = df.withColumn(
            filter_rule_col_name,
            F.when(
                F.col(clean_col_name).isNull()
                & (F.col(filter_rule_col_name) == winsorized_rule),
                F.lit(new_rule_name),
            ).otherwise(F.col(filter_rule_col_name)),
        )

    return df


def aggregate_values_to_provider_level(df: DataFrame, col_to_sum: str) -> DataFrame:
    """
    Adds a new column with the provider level sum of a given column.
    The new column will be named col_to_sum suffixed with "_provider_sum".

    Args:
        df (DataFrame): A dataframe with providerid and the column_to_sum.
        col_to_sum (str): A column of values to sum.

    Returns:
        DataFrame: The input DataFrame with a new aggregated column.
    """
    w = Window.partitionBy([IndCQC.provider_id, IndCQC.cqc_location_import_date])
    df = df.withColumn(
        f"{col_to_sum}_provider_sum",
        F.sum(col_to_sum).over(w),
    )

    return df
