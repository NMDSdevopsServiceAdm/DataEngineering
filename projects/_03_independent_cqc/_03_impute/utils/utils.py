from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


def convert_care_home_ratios_to_posts(
    df: DataFrame, ratio_column: str, posts_column: str
) -> DataFrame:
    """
    Multiplies the filled posts per bed ratio values by the number of beds at each care home location to create a filled posts figure.

    If the location is not a care home, the original filled posts figure is kept.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column (str): The name of the filled posts per bed ratio column (for care homes only).
        posts_column (str): The name of the filled posts column.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        posts_column,
        F.when(
            F.col(IndCQC.care_home) == CareHome.care_home,
            F.col(ratio_column) * F.col(IndCQC.number_of_beds),
        ).otherwise(F.col(posts_column)),
    )
    return df


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


def nullify_ct_values_previous_to_first_submission(df: DataFrame) -> DataFrame:
    """
    Nullifies Capacity Tracker (CT) values for all dates prior to the first
    submission date.

    This is to ensure that we do not impute filled posts prior to collecting CT
    data.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with Capacity Tracker values nullified
        for all dates prior to collecting CT data.
    """
    before_first_submission = F.col(IndCQC.cqc_location_import_date) < date(2021, 5, 1)

    return df.withColumns(
        {
            IndCQC.ct_care_home_total_employed_imputed: F.when(
                before_first_submission,
                None,
            ).otherwise(F.col(IndCQC.ct_care_home_total_employed_imputed)),
            IndCQC.ct_non_res_care_workers_employed_imputed: F.when(
                before_first_submission,
                None,
            ).otherwise(F.col(IndCQC.ct_non_res_care_workers_employed_imputed)),
        }
    )
