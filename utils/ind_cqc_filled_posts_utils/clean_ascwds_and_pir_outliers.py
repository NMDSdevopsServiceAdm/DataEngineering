from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)
from utils.utils import select_rows_with_value


def clean_ascwds_and_pir_outliers(df: DataFrame) -> DataFrame:
    """
    Handles the steps for cleaning outliers when comparing ascwds_fillde_posts_dedup_clean and people_directly_employed_dedup.

    This function applies the cleaning steps to non res data only.

    Args:
        df(DataFrame): A dataframe containing the columns ascwds_filled_posts_dedup_clean and people_directly_employed_dedup

    Returns:
        DataFrame: A dataframe with outliers cleaned
    """
    care_home_df = select_rows_with_value(df, IndCQC.care_home, CareHome.care_home)
    non_res_df = select_rows_with_value(df, IndCQC.care_home, CareHome.not_care_home)
    non_res_df = check_rows_where_ascwds_less_than_people_directly_employed(non_res_df)
    cleaned_df = care_home_df.unionByName(non_res_df)
    return cleaned_df


def check_rows_where_ascwds_less_than_people_directly_employed(
    non_res_df: DataFrame,
) -> DataFrame:
    """
    Compares ascwds_filled_posts_dedup_clean and people_directly_employed_dedup but does not change the data.

    This function is an example to set up the code for PIR filtering and will be replaced.

    Args:
        df(DataFrame): A dataframe containing the columns ascwds_filled_posts_dedup_clean and people_directly_employed_dedup

    Returns:
        DataFrame: An unchanged dataframe.
    """
    non_res_df = non_res_df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            non_res_df[IndCQC.ascwds_filled_posts_dedup_clean]
            >= non_res_df[IndCQC.people_directly_employed_dedup],
            F.col(IndCQC.ascwds_filled_posts_dedup_clean),
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )
    # Update filtering rule here
    non_res_df = non_res_df.withColumn(
        IndCQC.people_directly_employed_dedup,
        F.when(
            non_res_df[IndCQC.ascwds_filtering_rule] != "example_rule",
            F.col(IndCQC.people_directly_employed_dedup),
        ).otherwise(F.col(IndCQC.people_directly_employed_dedup)),
    )

    return non_res_df
