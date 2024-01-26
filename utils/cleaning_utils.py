from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def apply_categorical_labels(
    df: DataFrame, labels: dict, columns: list, new_column: bool
) -> DataFrame:
    new_column_names = []
    for i, column_name in enumerate(columns):
        print(i)
        keys = list(labels[column_name].keys())
        values = list(labels[column_name].values())
        print(keys, values)
        new_column_names.append(column_name + "_labels")

        df = df.withColumn(new_column_names[i], F.col(column_name))
        df = df.na.replace(keys, values, new_column_names[i])
    return df
