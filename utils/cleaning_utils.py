from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def apply_categorical_labels(
    df: DataFrame, labels: dict, columns: list, new_column: bool = True
) -> DataFrame:
    if (new_column == True):
        for column_name in columns:
            new_column_name = column_name + "_labels"
            df = df.withColumn(new_column_name, F.col(column_name))
            df = replace_labels(df, labels, column_name, new_column_name)
    elif (new_column == False):
        for column_name in columns:
            df = replace_labels(df, labels, column_name)
    return df

def replace_labels(df: DataFrame, labels: dict, column_name:str, new_column_name:str = None):
    if (new_column_name == None):
        new_column_name = column_name
    keys = list(labels[column_name].keys())
    values = list(labels[column_name].values())
    df = df.na.replace(keys, values, new_column_name)
    return df
