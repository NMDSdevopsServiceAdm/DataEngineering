from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import (
    IntegerType,
    StringType,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_source_description_to_source_column(
    input_df: DataFrame,
    populated_column_name: str,
    source_column_name: str,
    source_description: str,
) -> DataFrame:
    return input_df.withColumn(
        source_column_name,
        F.when(
            (
                F.col(populated_column_name).isNotNull()
                & F.col(source_column_name).isNull()
            ),
            source_description,
        ).otherwise(F.col(source_column_name)),
    )


def populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
    df: DataFrame,
    order_of_models_to_populate_estimate_filled_posts_with: list,
    estimates_column_to_populate: str,
    estimates_source_column_to_populate: str,
) -> DataFrame:
    """
    Populate the estimates and source columns using the hierarchy provided in the list.

    The first item in the list is populated first. Subsequent gaps are filled by the following items in the list.

    Args:
        df(DataFrame): A data frame with estimates that need merging into a single column.
        order_of_models_to_populate_estimate_filled_posts_with (list): A list of column names of models.
        estimates_column_to_populate (str): The name of the column to populate with estimates.
        estimates_source_column_to_populate (str): The name of the column to populate with estimate sources.

    Returns:
        DataFrame: A data frame with estimates and estimates source column populated.
    """
    df = df.withColumn(estimates_column_to_populate, F.lit(None).cast(IntegerType()))
    df = df.withColumn(
        estimates_source_column_to_populate, F.lit(None).cast(StringType())
    )

    # TODO - replace for loop with better functionality
    # see https://trello.com/c/94jAj8cd/428-update-how-we-populate-estimates
    for model_name in order_of_models_to_populate_estimate_filled_posts_with:
        df = df.withColumn(
            estimates_column_to_populate,
            F.when(
                (
                    F.col(estimates_column_to_populate).isNull()
                    & (F.col(model_name).isNotNull())
                    & (F.col(model_name) >= 1.0)
                ),
                F.col(model_name),
            ).otherwise(F.col(estimates_column_to_populate)),
        )

        df = add_source_description_to_source_column(
            df,
            estimates_column_to_populate,
            estimates_source_column_to_populate,
            model_name,
        )

    return df


def get_selected_value(
    df: DataFrame,
    window_spec: Window,
    column_with_null_values: str,
    column_with_data: str,
    new_column: str,
    selection: str,
) -> DataFrame:
    """
    Creates a new column with the selected value (first or last) from a given column.

    This function creates a new column by selecting a specified value over a given window on a given dataframe. It will
    only select values in the column with data that have null values in the original column.

    Args:
        df (DataFrame): A dataframe containing the supplied columns.
        window_spec (Window): A window describing how to prepare the dataframe.
        column_with_null_values (str): A column with missing data.
        column_with_data (str): A column with data for all the rows that column_with_null_values has data. This can be column_with_null_values itself.
        new_column (str): The name of the new column containing the resulting selected values.
        selection (str): One of 'first' or 'last'. This determines which pyspark window function will be used.

    Returns:
        DataFrame: A dataframe containing a new column with the selected value populated through each window.

    Raises:
        ValueError: If 'selection' is not one of the two permitted pyspark window functions.
    """
    selection_methods = {"first": F.first, "last": F.last}

    if selection not in selection_methods:
        raise ValueError(
            f"Error: The selection parameter '{selection}' was not found. Please use 'first' or 'last'."
        )

    method = selection_methods[selection]

    df = df.withColumn(
        new_column,
        method(
            F.when(F.col(column_with_null_values).isNotNull(), F.col(column_with_data)),
            ignorenulls=True,
        ).over(window_spec),
    )

    return df
