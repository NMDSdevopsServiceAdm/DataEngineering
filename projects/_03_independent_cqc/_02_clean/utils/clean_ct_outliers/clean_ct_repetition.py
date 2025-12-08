from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import projects._03_independent_cqc.utils.utils.utils as utils
from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    update_filtering_rule,
)
from projects._03_independent_cqc._02_clean.utils.utils import (
    create_column_with_repeated_values_removed,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CTCareHomeFilteringRule,
    CTNonResFilteringRule,
)

# These dicts are required in clean_value_repetition function.
DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES = {
    0: 250,  # using 250 as proxy for 8 months.
    10: 125,  # using 125 as proxy for 4 months.
    50: 65,  # using 65 as proxy for 2 months.
}
DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES = {
    0: 370,  # using 370 as proxy for 12 months.
    10: 155,  # using 155 as proxy for 5 months.
    50: 125,
    250: 65,
}


def clean_ct_values_after_consecutive_repetition(
    df: DataFrame,
    column_to_clean: str,
    cleaned_column_name: str,
    care_home: bool,
    partitioning_column: str,
) -> DataFrame:
    """
    Nulls Capacity Tracker values after they have consecutively repeated for too long.

    When a value is repeated for more than a repetition limit, then the value is nulled after
    the limit period until a different value has been submitted.
    The repetition limit is based on the 75th percentile of the mean days that values stayed the same
    per location in Capacity Tracker data between December 2020 and November 2025.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): A column with repeated values.
        cleaned_column_name (str): A column with cleaned values.
        care_home (bool): True when cleaning care home values, False when cleaning non-residential values.
        partitioning_column (str): The column to partition by when deduplicating column_to_clean.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = CTCareHomeFilteringRule.location_repeats_total_posts
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.location_repeats_total_posts

    df_populated_only = df.filter(F.col(filter_rule_column_name) == populated_rule)

    df_populated_only = create_column_with_repeated_values_removed(
        df_populated_only,
        column_to_clean,
        f"{column_to_clean}_deduplicated",
        partitioning_column,
    )

    df_populated_only = calculate_days_a_value_has_been_repeated(
        df_populated_only, f"{column_to_clean}_deduplicated", IndCQC.location_id
    )

    df_populated_only = clean_value_repetition(
        df_populated_only, column_to_clean, care_home
    )

    df_cleaned = join_cleaned_ct_values_into_original_df(
        df, df_populated_only, cleaned_column_name
    )

    df_cleaned = update_filtering_rule(
        df_cleaned,
        filter_rule_column_name,
        column_to_clean,
        cleaned_column_name,
        populated_rule,
        new_rule_name,
    )

    return df_cleaned


def calculate_days_a_value_has_been_repeated(
    df: DataFrame,
    deduplicated_values_column: str,
    partitioning_column: str,
) -> DataFrame:
    """
    Adds a column with the number of days between import date on the row and the import date of the last known value
    when import date is in ascending order.

    Args:
        df (DataFrame): A dataframe with import date and a deduplicated value column.
        deduplicated_values_column (str): A column with repeated values removed.
        partitioning_column (str): The column to partition by when getting import date when values were first submitted.

    Returns:
        DataFrame: The input DataFrame with a new column days_since_previous_submission.
    """
    window_spec_backwards = (
        Window.partitionBy(partitioning_column)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = utils.get_selected_value(
        df,
        window_spec_backwards,
        deduplicated_values_column,
        IndCQC.cqc_location_import_date,
        "date_when_repeated_value_was_first_submitted",
        "last",
    )

    df = df.withColumn(
        "days_value_has_been_repeated",
        F.date_diff(
            IndCQC.cqc_location_import_date,
            "date_when_repeated_value_was_first_submitted",
        ),
    )

    return df.drop("date_when_repeated_value_was_first_submitted")


def clean_value_repetition(
    df: DataFrame,
    column_to_clean: str,
    care_home: bool,
) -> DataFrame:
    """
    Adds a new column repeated_values_nulled in which values from column_to_clean are nulled
    when days_value_has_been_repeated is above the limit.

    The limits are defined in a dictionary with keys = minimum posts and values = days limit.
    Analysis of Capacity Tracker data showed non-residential and care home locations had different distributions
    of days they repeated values based on their size. Therefore, each type has its own limits.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): The column with repeated values.
        care_home (bool): True when cleaning care home values, False when cleaning non-residential values.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    if care_home:
        sorted_dict = sorted(
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES.items()
        )
    else:
        sorted_dict = sorted(
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES.items()
        )

    column_expression = None
    for key, value in sorted_dict:
        condition = F.col(column_to_clean) >= key
        column_expression = (
            value
            if column_expression is None
            else F.when(condition, value).otherwise(column_expression)
        )
    repetition_limit_based_on_posts = "repetition_limit_based_on_posts"
    df = df.withColumn(repetition_limit_based_on_posts, column_expression)

    df = df.withColumn(
        "repeated_values_nulled",
        F.when(
            F.col("days_value_has_been_repeated")
            <= F.col(repetition_limit_based_on_posts),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    return df.drop("repetition_limit_based_on_posts")


def join_cleaned_ct_values_into_original_df(
    original_df: DataFrame, populated_only_df: DataFrame, cleaned_column_name: str
) -> DataFrame:
    """
    Joins the original DataFrame with the populated only DataFrame.

    The repeated_values_nulled column is added to the original DataFrame, then this column replaces the cleaned column from the orginal DataFrame.

    Args:
        original_df (DataFrame): The DataFrame as it was before and repeated values cleaning steps.
        populated_only_df (DataFrame): A DataFrame with a new cleaned values column.
        cleaned_column_name (str): The name of cleaned values column.

    Returns:
        DataFrame: The original DataFrame with the cleaned values column replaced by the version in populated only DataFrame.
    """
    cols_to_select_before_join = [
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        "repeated_values_nulled",
    ]
    populated_only_df = populated_only_df.select(cols_to_select_before_join)

    cleaned_df = original_df.join(
        populated_only_df,
        on=[IndCQC.location_id, IndCQC.cqc_location_import_date],
        how="left",
    )

    cleaned_df = cleaned_df.withColumn(
        cleaned_column_name, F.col("repeated_values_nulled")
    )

    return cleaned_df.drop("repeated_values_nulled")
