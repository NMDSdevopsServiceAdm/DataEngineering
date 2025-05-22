from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import IntegerType, StringType, MapType, DoubleType
from typing import List
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    Dormancy,
    PrimaryServiceType,
)


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


# TODO Remove this function if it has been replaced and is no longer used.
def populate_estimate_filled_posts_and_source_in_the_order_of_the_column_list(
    df: DataFrame,
    order_of_models_to_populate_estimate_filled_posts_with: list,
    estimates_column_to_populate: str,
    estimates_source_column_to_populate: str,
) -> DataFrame:
    """
    Populate the estimates and source columns using the hierarchy provided in the list.

    The first item in the list is populated first. Subsequent null values are filled by the following items in the list.

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


def merge_columns_in_order(
    df: DataFrame,
    ordered_list_of_columns_to_be_merged: List,
    merged_column_name: str,
    merged_column_source_name: str,
) -> DataFrame:
    """
    Merges a given list of columns into a new column and adds another column for the source.

    This function creates a new column using the values from other columns given in a list.
    Values are taken from the given list of columns in the order of the list.
    The given list of columns must all be of the same datatype which can be float or map.
    Float values will only be taken if they are greater than or equal to 1.0.
    Map elements will only be taken if they are not null.
    This function also adds a new column for the source, which is the column name that values were taken from.

    Args:
        df (DataFrame): A dataframe containing multiple columns of job role ratios.
        ordered_list_of_columns_to_be_merged (List): A list of column names in priority order highest to lowest.
        merged_column_name (str): The name to give the new merged column.
        merged_column_source_name (str): The name to give the new merged source column.

    Returns:
        DataFrame: A dataframe with a column for the merged job role ratios.

    Raises:
        ValueError: If the given list of columns are not all 'double' or all 'map' datatypes.
    """
    column_types = list(
        set(
            [
                df.schema[column].dataType
                for column in ordered_list_of_columns_to_be_merged
            ]
        )
    )
    if len(column_types) > 1:
        raise ValueError(
            f"The columns to merge must all have the same datatype. Found {column_types}."
        )

    if isinstance(column_types[0], DoubleType):
        df = df.withColumn(
            merged_column_name,
            F.coalesce(
                *[
                    F.when((F.col(column) >= 1.0), F.col(column))
                    for column in ordered_list_of_columns_to_be_merged
                ]
            ),
        )

        source_column = F.when(
            F.col(ordered_list_of_columns_to_be_merged[0]) >= 1.0,
            ordered_list_of_columns_to_be_merged[0],
        )
        for column_name in ordered_list_of_columns_to_be_merged[1:]:
            source_column = source_column.when(F.col(column_name) >= 1.0, column_name)

    elif isinstance(column_types[0], MapType):
        df = df.withColumn(
            merged_column_name,
            F.coalesce(
                *[F.col(column) for column in ordered_list_of_columns_to_be_merged]
            ),
        )

        source_column = F.when(
            F.col(ordered_list_of_columns_to_be_merged[0]).isNotNull(),
            ordered_list_of_columns_to_be_merged[0],
        )
        for column_name in ordered_list_of_columns_to_be_merged[1:]:
            source_column = source_column.when(
                F.col(column_name).isNotNull(), column_name
            )

    else:
        raise ValueError(
            f"Columns to merge must be either 'double' or 'map' type. Found {column_types}."
        )

    df = df.withColumn(merged_column_source_name, source_column)

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


def copy_and_fill_filled_posts_when_becoming_not_dormant(df: DataFrame) -> DataFrame:
    """
    Copy estimate_filled_posts into new column when non-residential location becomes non-dorment.

    At the point dormancy changes from 'Y' to 'N', copy the estimate_filled_posts value from the
    'Y' period into a new column at that 'N' period row. Then copy that value into the previous period
    (where dormancy was 'Y').

    Args:
        df (DataFrame): A dataframe with estimate_filled_posts and dormancy

    Returns:
        DataFrame: A dataframe with a new column estimated_filled_posts_at_point_of_becoming_non_dormant.
    """

    w = Window.partitionBy(IndCQC.location_id).orderBy(IndCQC.cqc_location_import_date)

    new_column = IndCQC.estimated_filled_posts_at_point_of_becoming_non_dormant
    prev_dormancy = F.lag(IndCQC.dormancy).over(w)
    current_dormancy = F.col(IndCQC.dormancy)
    prev_filled_posts = F.lag(IndCQC.estimate_filled_posts).over(w)
    next_estimated_posts = F.lead(new_column).over(w)

    df = df.withColumn(
        new_column,
        F.when(
            (F.col(IndCQC.primary_service_type) == PrimaryServiceType.non_residential)
            & (prev_dormancy == Dormancy.dormant)
            & (current_dormancy == Dormancy.not_dormant),
            prev_filled_posts,
        ),
    )

    df = df.withColumn(
        new_column,
        F.when(next_estimated_posts.isNotNull(), next_estimated_posts).otherwise(
            F.col(new_column)
        ),
    )

    return df


def overwrite_estimate_filled_posts_with_imputed_estimated_filled_posts_at_point_of_becoming_non_dormant(
    df: DataFrame,
) -> DataFrame:
    """
    Overwrite the estimate_filled_posts column with estimated_filled_posts_at_point_of_becoming_non_dormant.

    Args:
        df (DataFrame): A dataframe with estimated_filled_posts_at_point_of_becoming_non_dormant and estimate_filled_posts.

    Returns:
        DataFrame: A dataframe with estimate_filled_posts overwritten by estimated_filled_posts_at_point_of_becoming_non_dormant.
    """

    df = df.withColumn(
        IndCQC.estimate_filled_posts,
        F.coalesce(
            F.col(
                IndCQC.imputed_estimated_filled_posts_at_point_of_becoming_non_dormant
            ),
            F.col(IndCQC.estimate_filled_posts),
        ),
    )

    return df
