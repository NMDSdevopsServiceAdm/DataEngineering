from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
import pyspark.sql.functions as F

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    PartitionKeys as Keys,
)

key: str = "key"
value: str = "value"


def apply_categorical_labels(
    df: DataFrame,
    spark: SparkSession,
    labels: dict,
    column_names: list,
    add_as_new_column: bool = True,
) -> DataFrame:
    for column_name in column_names:
        labels_df = convert_labels_dict_to_dataframe(labels[column_name], spark)
        if add_as_new_column == True:
            new_column_name = column_name + "_labels"
            df = replace_labels(df, labels_df, column_name, new_column_name)
        elif add_as_new_column == False:
            df = replace_labels(df, labels_df, column_name)
    return df


def replace_labels(
    df: DataFrame, labels_df: DataFrame, column_name: str, new_column_name: str = None
) -> DataFrame:
    df = df.join(labels_df, [df[column_name] == labels_df[key]], how="left")
    df = drop_unecessary_columns(df, column_name, new_column_name)
    return df


def drop_unecessary_columns(
    df: DataFrame, column_name: str, new_column_name: str = None
) -> DataFrame:
    if new_column_name == None:
        new_column_name = column_name
        df = df.drop(key, column_name)
    else:
        df = df.drop(key)
    df = df.withColumnRenamed(value, new_column_name)

    return df


def convert_labels_dict_to_dataframe(labels: dict, spark: SparkSession) -> DataFrame:
    labels_schema = StructType(
        [
            StructField(key, StringType(), True),
            StructField(value, StringType(), True),
        ]
    )
    labels_df = spark.createDataFrame(labels, labels_schema)
    return labels_df


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


def align_import_dates(primary_df:DataFrame, secondary_df: DataFrame, primary_date_column:str=Keys.import_date, secondary_date_column:str=Keys.import_date) -> DataFrame:
    aligned_dates_df = primary_df
    return aligned_dates_df

def join_on_misaligned_import_dates(primary_df:DataFrame, secondary_df: DataFrame, aligned_dates:DataFrame) -> DataFrame:
    joined_df = primary_df
    return joined_df