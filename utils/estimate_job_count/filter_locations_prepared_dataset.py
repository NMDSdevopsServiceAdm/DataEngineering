import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def filter_to_only_cqc_independent_sector_data(df: DataFrame) -> DataFrame:
    return df.where(df.cqc_sector == "Independent")
