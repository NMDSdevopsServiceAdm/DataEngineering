from pyspark.sql import DataFrame, functions as F, Window
from typing import Optional, Tuple
from functools import reduce

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.estimate_filled_posts_by_job_role_utils.utils import (
    unpack_mapped_column,
    create_map_column,
)

import utils.estimate_filled_posts.models.interpolation as interpolation


def model_job_role_ratio_interpolation(
    df: DataFrame,
) -> DataFrame:
    """
    Performs interpolation on ascwds_job_role_ratio column

    Args:
        df (DataFrame): The input DataFrame containing the columng ascwds_job_role_ratio column

    Returns:
        DataFrame: The DataFrame with the ascwds_job_role_ratio_interpolated column

    """

    df_to_interpolate = unpack_mapped_column(df, IndCqc.ascwds_job_role_ratios)

    df_keys = df_to_interpolate.select(
        F.explode(F.map_keys(F.col(IndCqc.ascwds_job_role_ratios)))
    ).distinct()
    columns_to_interpolate = sorted([row[0] for row in df_keys.collect()])

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCqc.ascwds_job_role_ratios_temporary,
        create_map_column(columns_to_interpolate),
    )

    df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

    df_to_interpolate = df_to_interpolate.select(
        IndCqc.location_id,
        IndCqc.unix_time,
        F.explode(IndCqc.ascwds_job_role_ratios_temporary).alias("key", "ratios"),
    )

    partitions = [IndCqc.location_id, "key"]

    df_to_interpolate = interpolation.model_interpolation(
        df_to_interpolate,
        "ratios",
        "straight",
        IndCqc.ascwds_job_role_ratios_interpolated,
        partitions,
    )

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCqc.ascwds_job_role_ratios_interpolated,
        F.coalesce(F.col(IndCqc.ascwds_job_role_ratios_interpolated), F.col("ratios")),
    )
    print("new point!")
    df_to_interpolate.show()

    df_to_interpolate = (
        df_to_interpolate.groupBy(IndCqc.location_id, IndCqc.unix_time)
        .pivot("key")
        .agg(F.first(IndCqc.ascwds_job_role_ratios_interpolated, ignorenulls=False))
    )
    print("next new point!")
    df_to_interpolate.show()

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCqc.ascwds_job_role_ratios_interpolated,
        create_map_column(columns_to_interpolate),
    )

    df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

    df_result = df_to_interpolate.join(
        df, on=[IndCqc.location_id, IndCqc.unix_time], how="inner"
    )

    return df_result


# def model_job_role_ratio_interpolation(
#     df: DataFrame,
#     mapped_column_to_interpolate: str,
#     method: str,
# ) -> DataFrame:
#     """
#     Perform interpolation on a column with null values and adds as a new column called 'interpolation_model'.

#     This function can produce two styles of interpolation:
#         - straight line interpolation
#         - trend line interpolation (as part of imputation model), where it uses the extrapolation_forwards values as a trend line to guide interpolated predictions.

#     Args:
#         df (DataFrame): The input DataFrame containing the data.
#         mapped_column_to_interpolate (str): the mapped column we want to apply interpolation on
#         method (str): The choice of method. Must be either 'straight' or 'trend'

#     Returns:
#         DataFrame: The DataFrame with the interpolated values in the 'interpolation_model' column.

#     Raises:
#         ValueError: If chosen method does not match 'straight' or 'trend'.
#     """

#     print("df_start_count_of_rows:", df.count())

#     print("print_columns_before_unpacking:", df.columns)
#     # mapped column + individual columns
#     df_to_interpolate = unpack_mapped_column(df, mapped_column_to_interpolate)

#     print("print_columns_after_unpacking:", df_to_interpolate.columns)
#     print("df_to_interpolate_unpacked_map_count_of_rows:", df_to_interpolate.count())

#     df_keys = df_to_interpolate.select(
#         F.explode(F.map_keys(F.col(mapped_column_to_interpolate)))
#     ).distinct()
#     columns_to_interpolate = sorted([row[0] for row in df_keys.collect()])

#     df_to_interpolate.printSchema()

#     # create mapped column from individual column
#     df_to_interpolate = df_to_interpolate.withColumn(
#         IndCqc.ascwds_job_role_ratios_temporary,
#         create_map_column(columns_to_interpolate),
#     )

#     print("df_to_interpolate_packed_map_count_of_rows:", df_to_interpolate.count())

#     df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

#     df_to_interpolate = df_to_interpolate.select(
#         IndCqc.location_id,
#         IndCqc.unix_time,
#         F.explode(IndCqc.ascwds_job_role_ratios_temporary).alias("key", "ratios"),
#     )

#     print(
#         "df_to_interpolate_exploded_map_count_of_rows_divided_by_39:",
#         df_to_interpolate.count() / 39,
#     )

#     (
#         window_spec_backwards,
#         window_spec_forwards,
#         window_spec_lagged,
#     ) = define_window_specs()

#     df_to_interpolate = calculate_proportion_of_time_between_submissions(
#         df_to_interpolate, "ratios", window_spec_backwards, window_spec_forwards
#     )

#     # where im at now
#     if method == "trend":
#         df_to_interpolate = calculate_residuals(
#             df_to_interpolate,
#             "ratios",
#             IndCqc.extrapolation_forwards,
#             window_spec_forwards,
#         )
#         df_to_interpolate = calculate_interpolated_values(
#             df_to_interpolate,
#             IndCqc.extrapolation_forwards,
#         )

#     elif method == "straight":
#         df_to_interpolate = get_selected_value(
#             df_to_interpolate,
#             window_spec_lagged,
#             "ratios",
#             "ratios",
#             IndCqc.previous_non_null_value,
#             "last",
#         )

#         df_to_interpolate = calculate_residuals(
#             df_to_interpolate,
#             "ratios",
#             IndCqc.previous_non_null_value,
#             window_spec_forwards,
#         )

#         df_to_interpolate = calculate_interpolated_values(df_to_interpolate, "ratios")

#         print(
#             "df_to_interpolate_interpolated_exploded_map_count_of_rows_divided_by_39:",
#             df_to_interpolate.count() / 39,
#         )

#         df_to_interpolate = df_to_interpolate.drop(IndCqc.previous_non_null_value)

#     else:
#         raise ValueError("Error: method must be either 'straight' or 'trend'")

#     df_to_interpolate = (
#         df_to_interpolate.groupBy(IndCqc.location_id, IndCqc.unix_time)
#         .pivot("key")
#         .agg(F.first("ratios", ignorenulls=False))
#     )

#     print(
#         "df_to_interpolate_pivoted_count_of_rows_divided_by_39:",
#         df_to_interpolate.count(),
#     )

#     df_to_interpolate = df_to_interpolate.withColumn(
#         IndCqc.ascwds_job_role_ratios_interpolated,
#         create_map_column(columns_to_interpolate),
#     )

#     df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

#     df_result = df_to_interpolate.join(
#         df, on=[IndCqc.location_id, IndCqc.unix_time], how="inner"
#     )

#     print("df_to_interpolate_join_count_of_rows_divided_by_39:", df_result.count())

#     return df_result


# def define_window_specs() -> Tuple[Window, Window, Window]:
#     """
#     Defines three window specifications, partitioned by 'location_id' and ordered by 'unix_time'.

#     The first window specification ('window_spec_backwards') includes all rows up to the current row.
#     The second window specification ('window_spec_forward') includes all rows from the current row onwards.
#     The third window specification ('window_spec_lagged') includes all rows from the start of the partition up to the current row, excluding the current row.

#     Returns:
#         Tuple[Window, Window, Window]: A tuple containing the three window specifications.
#     """
#     window_spec = Window.partitionBy(IndCqc.location_id, "key").orderBy(
#         IndCqc.unix_time
#     )

#     window_spec_backwards = window_spec.rowsBetween(
#         Window.unboundedPreceding, Window.currentRow
#     )
#     window_spec_forward = window_spec.rowsBetween(
#         Window.currentRow, Window.unboundedFollowing
#     )
#     window_spec_lagged = window_spec.rowsBetween(Window.unboundedPreceding, -1)

#     return window_spec_backwards, window_spec_forward, window_spec_lagged


# def calculate_residuals(
#     df: DataFrame, first_column: str, second_column: str, window_spec_forward: Window
# ) -> DataFrame:
#     """
#     Calculate the residual between two non-null values (first_column minus second_column).

#     This function computes the residuals between two non-null values in the specified columns.
#     It creates a temporary column to store the difference between the non-null values, then duplicates
#     the first non-null residual over a specified window and assigns it to a new column called 'residual'.

#     Args:
#         df (DataFrame): The input DataFrame containing the data.
#         first_column (str): The name of the first column that contains values.
#         second_column (str): The name of the second column that contains values.
#         window_spec_forward (Window): The window specification for getting the next residual value.

#     Returns:
#         DataFrame: The DataFrame with the calculated residuals in a new column.
#     """
#     temp_col: str = "temp_col"
#     df = df.withColumn(
#         temp_col,
#         F.when(
#             F.col(first_column).isNotNull() & F.col(second_column).isNotNull(),
#             F.col(first_column) - F.col(second_column),
#         ),
#     )

#     df = df.withColumn(
#         IndCqc.residual,
#         F.when(
#             F.col(second_column).isNotNull(),
#             F.first(F.col(temp_col), ignorenulls=True).over(window_spec_forward),
#         ),
#     ).drop(temp_col)

#     return df


# def calculate_proportion_of_time_between_submissions(
#     df: DataFrame,
#     column_with_null_values: str,
#     window_spec_backwards: Window,
#     window_spec_forwards: Window,
# ) -> DataFrame:
#     """
#     Calculates the proportion of time, based on unix_time of each row, between two non-null submission times.

#     Args:
#         df (DataFrame): The input DataFrame containing the data.
#         column_with_null_values (str): The name of the column that contains null values.
#         window_spec_backwards (Window): The window specification for getting the unix_time of the previous non-null value.
#         window_spec_forwards (Window): The window specification for getting the unix_time of the next non-null value.

#     Returns:
#         DataFrame: The DataFrame with the new column added.
#     """
#     df = get_selected_value(
#         df,
#         window_spec_backwards,
#         column_with_null_values,
#         IndCqc.unix_time,
#         IndCqc.previous_submission_time,
#         "last",
#     )
#     df = get_selected_value(
#         df,
#         window_spec_forwards,
#         column_with_null_values,
#         IndCqc.unix_time,
#         IndCqc.next_submission_time,
#         "first",
#     )

#     df = df.withColumn(
#         IndCqc.proportion_of_time_between_submissions,
#         F.when(
#             (F.col(IndCqc.previous_submission_time) < F.col(IndCqc.unix_time))
#             & (F.col(IndCqc.next_submission_time) > F.col(IndCqc.unix_time)),
#             (F.col(IndCqc.unix_time) - F.col(IndCqc.previous_submission_time))
#             / (
#                 F.col(IndCqc.next_submission_time)
#                 - F.col(IndCqc.previous_submission_time)
#             ),
#         ),
#     ).drop(IndCqc.previous_submission_time, IndCqc.next_submission_time)

#     return df


# def calculate_interpolated_values(
#     df: DataFrame, column_to_interpolate_from: str
# ) -> DataFrame:
#     """
#     Update a column with interpolated values in a DataFrame.

#     This function takes a DataFrame and interpolates values from an existing column,
#     updating the column in place. The interpolation is based on the residual and
#     the proportion of time between submissions.

#     Args:
#         df (DataFrame): The input DataFrame containing the data.
#         column_to_interpolate_from (str): The name of the column to update with interpolated values.

#     Returns:
#         DataFrame: The DataFrame with the specified column updated.
#     """
#     df = df.withColumn(
#         column_to_interpolate_from,
#         F.coalesce(
#             F.col(column_to_interpolate_from),
#             F.col(IndCqc.previous_non_null_value)
#             + F.col(IndCqc.residual)
#             * F.col(IndCqc.proportion_of_time_between_submissions),
#         ),
#     )
#     return df
