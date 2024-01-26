from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def apply_categorical_labels(df:DataFrame, labels:dict, columns: list, new_column:bool) -> DataFrame:
    new_column_name = columns[0] + "labels"
    df = df.withColumn(new_column_name, F.col(columns[0]))
    return df