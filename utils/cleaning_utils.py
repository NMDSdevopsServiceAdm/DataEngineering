from pyspark.sql import (
    DataFrame, 
    SparkSession,
)
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
key:str = "key"
value:str = "value"
labels_schema = StructType(
        [
            StructField(key, StringType(), True),
            StructField(value, StringType(), True),
        ]
    )

def apply_categorical_labels(
    df: DataFrame, spark: SparkSession, labels: dict, column_names: list, add_as_new_column: bool = True
) -> DataFrame:
    for column_name in column_names:
        if (add_as_new_column == True):
            new_column_name = column_name + "_labels"
            df = replace_labels(df, spark, labels, column_name, new_column_name)
        elif (add_as_new_column == False):
            df = replace_labels(df, spark, labels, column_name)
    return df


def replace_labels(df: DataFrame, spark: SparkSession, labels: dict, column_name:str, new_column_name:str = None) -> DataFrame:
    
    labels_df = spark.createDataFrame(labels[column_name], labels_schema)
    df = df.join(labels_df, [df[column_name] == labels_df[key]], how="left")
    if (new_column_name == None):
        new_column_name = column_name
        df = df.drop(key, column_name)
        df = df.withColumnRenamed(value, new_column_name)
    else:
        df = df.withColumnRenamed(value, new_column_name)
        df = df.drop(key)

    return df

