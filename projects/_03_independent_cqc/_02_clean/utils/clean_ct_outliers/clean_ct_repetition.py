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

DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES = {
    0: 243,
    10: 122,
    50: 61,
}
DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES = {
    0: 395,
    10: 152,
    50: 122,
    250: 61,
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
    The repetition limit is based on the mean days that values stayed the same per non-residential location in
    Capacity Tracker data between December 2020 and November 2025.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): A column with repeated values.
        cleaned_column_name (str): A column with cleaned values.
        care_home (bool): True when cleaning care home values, False when cleaning non-residential values.
        partitioning_column (str): The column to partition by when deduplicating column_to_clean.

    Returns:
        DataFrame: The input with DataFrame with an additional column.
    """
    df = create_column_with_repeated_values_removed(
        df, column_to_clean, f"{column_to_clean}_deduplicated", partitioning_column
    )

    df = calculate_days_a_value_has_been_repeated(
        df, f"{column_to_clean}_deduplicated", IndCQC.location_id
    )

    df = clean_value_repetition(df, column_to_clean, cleaned_column_name, care_home)

    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = CTCareHomeFilteringRule.location_repeats_total_posts
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.location_repeats_total_posts

    df = update_filtering_rule(
        df,
        filter_rule_column_name,
        column_to_clean,
        cleaned_column_name,
        populated_rule,
        new_rule_name,
    )

    df = df.drop(
        f"{column_to_clean}_deduplicated",
        IndCQC.days_value_has_been_repeated,
    )

    return df


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
    window_spec = Window.partitionBy(partitioning_column).orderBy(
        IndCQC.cqc_location_import_date
    )
    window_spec_backwards = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df = utils.get_selected_value(
        df,
        window_spec_backwards,
        deduplicated_values_column,
        IndCQC.cqc_location_import_date,
        IndCQC.date_when_repeated_value_was_first_submitted,
        "last",
    )

    df = df.withColumn(
        IndCQC.days_value_has_been_repeated,
        F.date_diff(
            IndCQC.cqc_location_import_date,
            IndCQC.date_when_repeated_value_was_first_submitted,
        ),
    )

    return df.drop(IndCQC.date_when_repeated_value_was_first_submitted)


def clean_value_repetition(
    df: DataFrame,
    column_to_clean: str,
    cleaned_column_name: str,
    care_home: bool,
) -> DataFrame:
    """
    Nulls values in column_to_clean when days_value_has_been_repeated is above the limit.

    The limits are defined in a dictionary with keys = minimum posts and values = days limit.
    Analysis of Capacity Tracker data showed non-residential and care home locations had different distributions
    of days they repeated values based on their size. Therefore, each type has its own limits.

    Args:
        df (DataFrame): A dataframe with consecutive import dates.
        column_to_clean (str): The column with repeated values.
        cleaned_column_name (str): A column with cleaned values.
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
        cleaned_column_name,
        F.when(
            F.col(IndCQC.days_value_has_been_repeated)
            <= F.col(repetition_limit_based_on_posts),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    return df.drop(repetition_limit_based_on_posts)
