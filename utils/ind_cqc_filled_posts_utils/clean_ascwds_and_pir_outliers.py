from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)
from utils.utils import select_rows_with_value


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
    care_home_df = select_rows_with_value(df, IndCQC.care_home, CareHome.care_home)
    non_res_df = select_rows_with_value(df, IndCQC.care_home, CareHome.not_care_home)
    non_res_df = non_res_df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (non_res_df[IndCQC.ascwds_filled_posts_dedup_clean].isNull())
            | (non_res_df[IndCQC.people_directly_employed_dedup].isNull())
            | (
                non_res_df[IndCQC.ascwds_filled_posts_dedup_clean]
                >= non_res_df[IndCQC.people_directly_employed_dedup]
            ),
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
        ),
    )
    non_res_df = non_res_df.withColumn(
        IndCQC.people_directly_employed_dedup,
        F.when(
            (non_res_df[IndCQC.ascwds_filled_posts_dedup_clean].isNull())
            | (non_res_df[IndCQC.people_directly_employed_dedup].isNull())
            | (
                non_res_df[IndCQC.ascwds_filled_posts_dedup_clean]
                >= non_res_df[IndCQC.people_directly_employed_dedup]
            ),
            F.col(IndCQC.people_directly_employed_dedup),
        ),
    )
    non_res_df = update_filtering_rule(
        non_res_df, AscwdsFilteringRule.less_than_people_directly_employed
    )
    cleaned_df = care_home_df.unionByName(non_res_df)
    return cleaned_df
