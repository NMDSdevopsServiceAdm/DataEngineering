from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
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
