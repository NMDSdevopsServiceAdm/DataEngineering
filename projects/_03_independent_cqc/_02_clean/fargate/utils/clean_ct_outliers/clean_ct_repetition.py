import polars as pl

import projects._03_independent_cqc.utils.utils.utils as utils
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from projects._03_independent_cqc._02_clean.fargate.utils.utils import (
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
    50: 125,  # using 125 as proxy for 4 months.
    250: 65,  # using 65 as proxy for 2 months.
}


def clean_ct_values_after_consecutive_repetition(
    lf: pl.LazyFrame,
    column_to_clean: str,
    cleaned_column_name: str,
    care_home: bool,
    partitioning_column: str,
) -> pl.LazyFrame:
    """
    Nulls Capacity Tracker values after they have consecutively repeated for too
    long.

    When a value is repeated for more than a repetition limit, then the value is
    nulled after the limit period until a different value has been submitted.
    The repetition limit is based on the 75th percentile of the mean days that
    values stayed the same per location in Capacity Tracker data between
    December 2020 and November 2025.

    Args:
        lf (pl.LazyFrame): A LazyFrame with consecutive import dates.
        column_to_clean (str): A column with repeated values.
        cleaned_column_name (str): A column with cleaned values.
        care_home (bool): True when cleaning care home values, False when cleaning
            non-residential values.
        partitioning_column (str): The column to partition by when deduplicating
            column_to_clean.

    Returns:
        pl.LazyFrame: LazyFrame with an additional column.
    """
    if care_home:
        repetition_limit_dict = (
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES
        )
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = CTCareHomeFilteringRule.location_repeats_total_posts
    else:
        repetition_limit_dict = (
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES
        )
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.location_repeats_total_posts

    deduplicated_col = f"{column_to_clean}_deduplicated"

    # Work on the full frame but scope windowed ops to populated rows only by
    # using .filter() inside the over() expression where needed.
    lf = create_column_with_repeated_values_removed(
        lf=lf,
        column_to_clean=column_to_clean,
        new_column_name=deduplicated_col,
        column_to_partition_by=partitioning_column,
    )

    # Mask the deduplicated column to null for non-populated rows so that the
    # backwards-looking "last non-null date" window ignores them.
    lf = lf.with_columns(
        pl.when(pl.col(filter_rule_column_name) == populated_rule)
        .then(pl.col(deduplicated_col))
        .otherwise(None)
        .alias(deduplicated_col)
    )

    lf = calculate_days_a_value_has_been_repeated(
        lf=lf,
        deduplicated_values_column=deduplicated_col,
        partitioning_column=IndCQC.location_id,
    )

    lf = clean_value_repetition(
        lf=lf,
        column_to_clean=column_to_clean,
        repetition_limit_dict=repetition_limit_dict,
    )

    # Apply cleaned values: populated rows get the nulled-if-repeated value;
    # all other rows get null (matching the original join behaviour).

    lf = lf.with_columns(
        pl.when(pl.col(filter_rule_column_name) == populated_rule)
        .then(pl.col("repeated_values_nulled"))
        .otherwise(None)
        .alias(cleaned_column_name)
    ).drop(["repeated_values_nulled", deduplicated_col, "days_value_has_been_repeated"])

    lf_cleaned = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=column_to_clean,
        clean_col_name=cleaned_column_name,
        populated_rule=populated_rule,
        new_rule_name=new_rule_name,
    )

    return lf_cleaned


def calculate_days_a_value_has_been_repeated(
    lf: pl.LazyFrame,
    deduplicated_values_column: str,
    partitioning_column: str,
) -> pl.LazyFrame:
    """
    Adds a column with the number of days between import date on the row and
    the import date when the current repeated value was first submitted.

    Args:
        lf (pl.LazyFrame): A LazyFrame with import date and a deduplicated value column.
        deduplicated_values_column (str): A column with repeated values removed (nulls where repeated).
        partitioning_column (str): The column to partition by.

    Returns:
        pl.LazyFrame: The input LazyFrame with a new column days_value_has_been_repeated.
    """
    # Get the import date of the last non-null deduplicated value (i.e. when the
    # current streak started). Because deduplicated_values_column is only non-null
    # on the first occurrence of each run, the last non-null date within the
    # backwards-looking window is exactly when the current value was first submitted.
    date_when_first_submitted = (
        pl.when(pl.col(deduplicated_values_column).is_not_null())
        .then(pl.col(IndCQC.cqc_location_import_date))
        .otherwise(None)
        .forward_fill()
        .over(
            partition_by=partitioning_column,
            order_by=IndCQC.cqc_location_import_date,
        )
    )

    return lf.with_columns(
        (pl.col(IndCQC.cqc_location_import_date) - date_when_first_submitted)
        .dt.total_days()
        .alias("days_value_has_been_repeated")
    )


def clean_value_repetition(
    lf: pl.LazyFrame,
    column_to_clean: str,
    repetition_limit_dict: dict[int, int],
) -> pl.LazyFrame:
    """
    Adds a new column repeated_values_nulled in which values from column_to_clean
    are nulled when days_value_has_been_repeated exceeds the limit for that post count.

    Args:
        lf (pl.LazyFrame): A LazyFrame with days_value_has_been_repeated column.
        column_to_clean (str): The column with post counts to check against limits.
        repetition_limit_dict (dict[int, int]): Keys = min posts, values = max days
            a posts value can be repeated.

    Returns:
        pl.LazyFrame: The input LazyFrame with an additional repeated_values_nulled column.
    """
    repetition_limits_sorted = sorted(repetition_limit_dict.items())

    # Build a pl.when/then/otherwise chain (innermost = smallest min_posts bucket).
    # We iterate in ascending order so larger post counts override smaller ones.
    limit_expr = pl.lit(repetition_limits_sorted[0][1])
    for min_posts, rep_limit in repetition_limits_sorted[1:]:
        limit_expr = (
            pl.when(pl.col(column_to_clean) >= min_posts)
            .then(pl.lit(rep_limit))
            .otherwise(limit_expr)
        )

    return lf.with_columns(
        pl.when(pl.col("days_value_has_been_repeated") <= limit_expr)
        .then(pl.col(column_to_clean))
        .otherwise(None)
        .alias("repeated_values_nulled")
    )
