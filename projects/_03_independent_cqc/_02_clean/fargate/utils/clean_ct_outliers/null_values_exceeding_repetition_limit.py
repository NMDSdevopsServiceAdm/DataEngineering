import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTFilteringRule

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


def null_values_exceeding_repetition_limit(
    lf: pl.LazyFrame,
    column_to_clean: str,
    care_home: bool,
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
        care_home (bool): True when cleaning care home values, False when cleaning
            non-residential values.

    Returns:
        pl.LazyFrame: LazyFrame with an additional column.
    """
    if care_home:
        repetition_limit_dict = (
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_CARE_HOMES
        )
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
    else:
        repetition_limit_dict = (
            DICT_OF_MINIMUM_POSTS_AND_MAX_REPETITION_DAYS_LOCATIONS_NON_RES
        )
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule

    limit_expr = repetition_limit_expr(column_to_clean, repetition_limit_dict)

    lf = lf.sort([IndCQC.location_id, IndCQC.cqc_location_import_date])

    streak_id = pl.col(column_to_clean).forward_fill().rle_id().over(IndCQC.location_id)

    streak_start = (
        pl.col(IndCQC.cqc_location_import_date)
        .min()
        .over([IndCQC.location_id, streak_id])
    )

    days_repeated = (
        pl.col(IndCQC.cqc_location_import_date) - streak_start
    ).dt.total_days()

    cleaned_expr = (
        pl.when(days_repeated <= limit_expr)
        .then(pl.col(column_to_clean))
        .otherwise(None)
    )

    lf = lf.with_columns(cleaned_expr.alias(column_to_clean))

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=column_to_clean,
        clean_col_name=column_to_clean,
        populated_rule=CTFilteringRule.populated,
        new_rule_name=CTFilteringRule.location_repeats_total_posts,
    )

    return lf


def repetition_limit_expr(
    column_to_clean: str,
    repetition_limit_dict: dict[int, int],
) -> pl.Expr:
    """
    Creates a Polars Expression which defines the repetition_limit based on
    'repetition_limit_dict'

    Args:
        column_to_clean (str): The column with post counts to check against
            repetition_limit_dict.
        repetition_limit_dict (dict[int, int]): Keys are min posts, values
            are max days a posts value can be repeated.

    Returns:
        pl.Expr: Polars Expression defining the repetition_limit.
    """
    repetition_limits_sorted = sorted(repetition_limit_dict.items())

    limit_expr = pl.lit(repetition_limits_sorted[0][1])
    for min_posts, rep_limit in repetition_limits_sorted[1:]:
        limit_expr = (
            pl.when(pl.col(column_to_clean) >= min_posts)
            .then(pl.lit(rep_limit))
            .otherwise(limit_expr)
        )

    return limit_expr
