from pyspark.sql import DataFrame, functions as F

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
)


def clean_people_directly_employed_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of the'pir_people_directly_employed' column.

    This function step duplicates 'pir_people_directly_employed' as 'pir_people_directly_employed_cleaned'
    and then runs various functions designed to remove outliers.

    Args:
        df (DataFrame): A dataframe containing 'pir_people_directly_employed'.

    Returns:
        DataFrame: A dataframe containing 'pir_people_directly_employed_cleaned'.
    """
    print("Cleaning pir_people_directly_employed...")

    df = df.withColumn(
        PIRCleanCols.pir_people_directly_employed_cleaned,
        F.col(PIRCols.pir_people_directly_employed),
    )

    # TODO only include locations with 100+ posts if they have 2 submission (so we can validate them against each other)

    # TODO remove locations with large differences (top 1% of (max-min)/avg?)

    return df
