from pyspark.sql import DataFrame, functions as F

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
)


def clean_ascwds_filled_post_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of 'ascwds_filled_posts_dedup' column.

    This function first duplicates 'ascwds_filled_posts_dedup' as 'ascwds_filled_posts_dedup_clean' and then runs
    various functions designed to clean ASCWDS filled post values.

    Args:
        df (DataFrame): A dataframe containing 'ascwds_filled_posts_dedup'.

    Returns:
        DataFrame: A dataframe containing 'ascwds_filled_posts_dedup_clean'.
    """
    print("Cleaning ascwds_filled_posts_dedup...")

    df = df.withColumnRenamed(
        PIRCols.pir_people_directly_employed,
        PIRCleanCols.pir_people_directly_employed_cleaned,
    )

    # df = add_filtering_rule_column(df)
    # df = null_filled_posts_where_locations_use_invalid_missing_data_code(df)
    # df = null_grouped_providers(df)
    # filtered_df = winsorize_care_home_filled_posts_per_bed_ratio_outliers(df)

    return filtered_df
