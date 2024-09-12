from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)

INVALID_MISSING_DATA_CODE = (
    999.0  # '999' is used elsewhere in ASCWDS to represent not known.
)


def null_filled_posts_where_locations_use_invalid_missing_data_code(
    df: DataFrame,
) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean values where locations have 999 in ascwds_filled_posts_dedup_clean.

    '999' is used elsewhere in ASCWDS to represent not known. Whilst that isn't the case for the variable totalstaff,
    upon investigation, all 999 entries do not look to be an accurate reflection of that location's staff so we are
    removing them for data quality.

    Args:
        df(DataFrame): A dataframe with ascwds_filled_posts_dedup_clean.

    Returns:
        DataFrame: A data frame with 999 ascwds_filled_posts_dedup_clean values nulled.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            df[IndCQC.ascwds_filled_posts_dedup_clean]
            != F.lit(INVALID_MISSING_DATA_CODE),
            df[IndCQC.ascwds_filled_posts_dedup_clean],
        ),
    )
    df = update_filtering_rule(
        df, AscwdsFilteringRule.contained_invalid_missing_data_code
    )
    return df
