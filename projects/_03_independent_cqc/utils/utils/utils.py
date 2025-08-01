from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import MapType, DoubleType
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    PrimaryServiceTypeSecondLevel as PSSL_values,
)
from utils.value_labels.ind_cqc_filled_posts.primary_service_type_mapping import (
    CqcServiceToPrimaryServiceTypeSecondLevelLookup as PSSL_lookup,
)


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


def allocate_primary_service_type_second_level(df: DataFrame) -> DataFrame:
    """
    Adds a column called primary_service_type_second_level which shows the allocated service type per location.

    The function builds a chain of when/otherwise clauses using the keys from the lookup_dict.
    The last key in the lookup_dict becomes the first when clause to evaluate.
    For example, when imputed_gac_service_types[description] == "Shared Lives" then return "Shared Lives".
    Therefore, the lookup_dict order determines which service type is allocated to a location.

    When none of the imputed_gac_service_types[description] are in the lookup_dict keys, then the row
    gets the default value 'Other non-residential'.

    Args:
        df (DataFrame): The input DataFrame containing the 'imputed_gac_service_types' column.

    Returns:
        DataFrame: The DataFrame with the new 'primary_service_type_second_level' column added.
    """

    lookup_dict = list(PSSL_lookup.dict.items())
    default_value = F.lit(PSSL_values.other_non_residential)

    condition = default_value
    for description, primary_service_type_second_level in lookup_dict:
        match_description = F.exists(
            F.col(IndCQC.imputed_gac_service_types),
            lambda x: x[CQCL.description] == description,
        )
        condition = F.when(
            match_description, primary_service_type_second_level
        ).otherwise(condition)

    df = df.withColumn(IndCQC.primary_service_type_second_level, condition)

    return df
