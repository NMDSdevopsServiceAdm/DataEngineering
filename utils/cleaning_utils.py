from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def apply_categorical_labels(df:DataFrame, labels:dict, columns: list, new_column:bool) -> DataFrame:
    new_column_names = []
    for i, column_name in enumerate(columns):
        print(i)
        new_column_names.append(column_name + "labels")
        # new_column_name = columns[0] + "labels"
        df = df.withColumn(new_column_names[i], F.col(column_name))
    return df