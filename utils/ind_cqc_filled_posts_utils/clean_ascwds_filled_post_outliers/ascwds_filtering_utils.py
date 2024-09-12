from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import (
    AscwdsFilteringRule,
)


def add_filtering_rule_column(df: DataFrame) -> DataFrame:
    """
    Add column which flags if data is present or missing.

    This function adds a new column which will eventually contain the filtering rules. When it is added, the data in ascwds_filled_posts_dedup_clean is identical to ascwds_filled_posts, so the only values will be "populated" and "missing data".

    Args:
        df (DataFrame): A dataframe containing ascwds_filled_posts_dedup_clean before any filters have been applied to the column.

    Returns:
        (DataFrame) : A dataframe with an additional column that states whether data is present or missing before filters are applied.
    """
    df = df.withColumn(
        IndCQC.ascwds_filtering_rule,
        F.when(
            F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNotNull(),
            F.lit(AscwdsFilteringRule.populated),
        ).otherwise(F.lit(AscwdsFilteringRule.missing_data)),
    )
    return df


def update_filtering_rule(df: DataFrame, rule_name: str) -> DataFrame:
    """
    Update filtering rule for rows where data has changed.

    This function updates the filtering rule in 2 cases:
    1) where it was listed as "populated" or a "winsorised_beds_ratio_outlier" but the current filtering rule has just nullified
    the data in ascwds_filled_posts_dedup_clean.
    2) where it was listed as "populated" and the cleaned data is different to when it was deduplicated.
    The new values will be the name of the filter passed to the function.

    Args:
        df (DataFrame): A dataframe containing ascwds_filled_posts_dedup, ascwds_filled_posts_dedup_clean and ascwds_filtering_rule after a new rule has been applied.
        rule_name (str): The name of the rule that has just been applied.

    Returns:
        (DataFrame) : A dataframe with the ascwds_filtering_rule column updated.
    """
    df = df.withColumn(
        IndCQC.ascwds_filtering_rule,
        F.when(
            (F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNull())
            & (
                (F.col(IndCQC.ascwds_filtering_rule) == AscwdsFilteringRule.populated)
                | (
                    F.col(IndCQC.ascwds_filtering_rule)
                    == AscwdsFilteringRule.winsorised_beds_ratio_outlier
                )
            ),
            F.lit(rule_name),
        )
        .when(
            (F.col(IndCQC.ascwds_filled_posts_dedup_clean).isNotNull())
            & (
                F.col(IndCQC.ascwds_filled_posts_dedup_clean)
                != (F.col(IndCQC.ascwds_filled_posts_dedup))
            )
            & (F.col(IndCQC.ascwds_filtering_rule) == AscwdsFilteringRule.populated),
            F.lit(rule_name),
        )
        .otherwise(F.col(IndCQC.ascwds_filtering_rule)),
    )
    return df
