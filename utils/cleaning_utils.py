from pyspark.sql import (
    DataFrame,
    Window,
)

import pyspark.sql.functions as F

key: str = "key"
value: str = "value"
import_date_s3_uri_format = "yyyyMMdd"


def apply_categorical_labels(
    df: DataFrame,
    labels: dict,
    column_names: list,
    add_as_new_column: bool = True,
) -> DataFrame:
    for column_name in column_names:
        labels_dict = labels[column_name]
        print(labels_dict)
        print(column_name)
        if add_as_new_column == True:
            new_column_name = column_name + "_labels"
            print(new_column_name)
            df.show()
            df = df.withColumn(new_column_name, F.col(column_name))
            df.show()
            df = df.replace(labels_dict, subset=new_column_name)
            df.show()
        elif add_as_new_column == False:
            df = df.replace(labels_dict, subset=column_name)
    return df






def set_column_bounds(
    df: DataFrame, col_name: str, new_col_name: str, lower_limit=None, upper_limit=None
):
    if lower_limit is None and upper_limit is None:
        return df

    if lower_limit > upper_limit:
        raise Exception(
            f"Lower limit ({lower_limit}) must be lower than upper limit ({upper_limit})"
        )

    if lower_limit is not None:
        df = df.withColumn(
            new_col_name,
            F.when(F.col(col_name) >= lower_limit, F.col(col_name)).otherwise(None),
        )
        col_name = new_col_name

    if upper_limit is not None:
        df = df.withColumn(
            new_col_name,
            F.when(F.col(col_name) <= upper_limit, F.col(col_name)).otherwise(None),
        )

    return df


def set_bounds_for_columns(
    df: DataFrame,
    col_names: list,
    new_col_names: list,
    lower_limit=None,
    upper_limit=None,
):
    if len(col_names) != len(new_col_names):
        raise Exception(
            f"Column list size ({len(col_names)}) must match new column list size ({len(new_col_names)})"
        )

    for col, new_col in zip(col_names, new_col_names):
        df = set_column_bounds(df, col, new_col, lower_limit, upper_limit)

    return df


def column_to_date(
    df: DataFrame,
    column_to_format: str,
    new_column: str = None,
    string_format: str = import_date_s3_uri_format,
) -> DataFrame:
    if new_column is None:
        new_column = column_to_format

    new_df = df.withColumn(new_column, F.to_date(column_to_format, string_format))

    return new_df


def align_import_dates(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    possible_matches = cross_join_unique_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    aligned_dates = determine_best_date_matches(
        possible_matches, primary_column, secondary_column
    )

    return aligned_dates


def cross_join_unique_dates(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    primary_dates = primary_df.select(primary_column).dropDuplicates()
    secondary_dates = secondary_df.select(secondary_column).dropDuplicates()

    possible_matches = primary_dates.crossJoin(secondary_dates).repartition(
        primary_column
    )
    return possible_matches


def determine_best_date_matches(
    possible_matches: DataFrame, primary_column: str, secondary_column: str
) -> DataFrame:
    date_diff: str = "date_diff"
    min_date_diff: str = "min_date_diff"
    possible_matches = possible_matches.withColumn(
        date_diff, F.datediff(primary_column, secondary_column)
    )

    possible_matches = possible_matches.where(possible_matches[date_diff] >= 0)

    w = Window.partitionBy(primary_column).orderBy(date_diff)
    possible_matches = possible_matches.withColumn(
        min_date_diff, F.min(date_diff).over(w)
    )

    aligned_dates = possible_matches.where(
        possible_matches[min_date_diff] == possible_matches[date_diff]
    )

    return aligned_dates.select(primary_column, secondary_column)


def add_aligned_date_column(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_column: str,
    secondary_column: str,
) -> DataFrame:
    aligned_dates = align_import_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    primary_df_with_aligned_dates = primary_df.join(
        aligned_dates, primary_column, "left"
    )

    return primary_df_with_aligned_dates
