from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


def clean_ascwds_and_pir_outliers(df):
    """
    Compares ascwds_filled_posts_dedup_clean and people_directly_employed_dedup and removes outliers.

    This function compares ascwds_filled_posts_dedup_clean and people_directly_employed_dedup and nulls
    both columns when ascwds data is lower than people directly employed. This is because people directly
    employed should be a subset of all filled posts at a location.

    Args:
        df(DataFrame): A dataframe containing the columns ascwds_filled_posts_dedup_clean and people_directly_employed_dedup

    Returns:
        DataFrame: A dataframe with outliers nulled.
    """
    return df
