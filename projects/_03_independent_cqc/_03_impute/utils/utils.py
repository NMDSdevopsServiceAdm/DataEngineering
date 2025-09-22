from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


def combine_care_home_and_non_res_values_into_single_column(
    df: DataFrame, care_home_column: str, non_res_column: str, new_column_name: str
) -> DataFrame:
    """
    Adds a new column which has the care_home_column values if the location is a care home and the non_res_column values if not.

    Args:
        df (DataFrame): The input DataFrame.
        care_home_column (str): The name of the column containing care home values.
        non_res_column (str): The name of the column containing non-res values.
        new_column_name (str): The name of the new column with combined values.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        new_column_name,
        F.when(
            F.col(IndCQC.care_home) == CareHome.care_home,
            F.col(care_home_column),
        ).otherwise(F.col(non_res_column)),
    )
    return df
