from dataclasses import dataclass

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class ThresholdValues:
    two_years: str = "-2y"
    max_absolute_difference: int = 100
    max_percentage_difference: float = 0.5


def merge_ascwds_and_pir_filled_post_submissions(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merges ASCWDS and PIR filled post estimates based on recently and similarity thresholds and stores in a new column.

    The ASCWDS dataset is the preferred source for workforce filled post figures.
    However, if a workplace has not submitted ASCWDS data for a prolonged period of time and the corresponding PIR
    submission differs significantly (both in absolute and percentage terms) then the PIR figure is used instead.
    This ensures that downstream imputation uses the most reliable and recent data.

    If the PIR and ASCWDS values are within acceptable difference thresholds, the older ASCWDS value is retained.
    This avoids introducing noise when the PIR and ASCWDS values are effectively aligned.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing filled posts from ASCWDS and PIR and their respective submission dates.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column `ascwds_pir_merged` that contains either the ASCWDS or PIR filled posts value,
                      depending on submission recency and similarity thresholds.
    """
    temp_date_cols = [IndCQC.last_ascwds_submission, IndCQC.last_pir_submission]

    lf = create_last_submission_columns(lf)
    lf_within_two_years, lf_outside_two_years = split_dataset_for_merging(lf)

    lf_within_two_years = lf_within_two_years.drop(temp_date_cols)
    lf_outside_two_years = lf_outside_two_years.drop(temp_date_cols)

    # lf_within_two_years = create_repeated_ascwds_clean_column(lf_within_two_years)
    # lf_within_two_years = create_ascwds_pir_merged_column(lf_within_two_years)
    # lf_within_two_years = lf_within_two_years.drop(
    #     IndCQC.ascwds_filled_posts_dedup_clean_repeated
    # )

    # lf_outside_two_years = include_pir_if_never_submitted_ascwds(lf_outside_two_years)

    return pl.concat([lf_within_two_years, lf_outside_two_years])


def create_last_submission_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds columns with the max cqc_location_import_date per location_id for
    ascwds and pir data.

    This column is needed to identify whether there has been a gap of at least two years
    between an ascwds submission and a pir submission.

    polars_streaming - .over() is not streaming compatible as at 13/07/2026

    Args:
        lf (pl.LazyFrame): A LazyFrame with ascwds and pir data.

    Returns:
        pl.LazyFrame: A LazyFrame with two extra columns containing the latest submission dates.
    """
    last_ascwds = (
        lf.filter(pl.col(IndCQC.ascwds_filled_posts_dedup_clean).is_not_null())
        .group_by(IndCQC.location_id)
        .agg(
            pl.col(IndCQC.cqc_location_import_date)
            .max()
            .alias(IndCQC.last_ascwds_submission)
        )
    )

    last_pir = (
        lf.filter(pl.col(IndCQC.pir_filled_posts_model).is_not_null())
        .group_by(IndCQC.location_id)
        .agg(
            pl.col(IndCQC.cqc_location_import_date)
            .max()
            .alias(IndCQC.last_pir_submission)
        )
    )

    lf = lf.join(last_ascwds, on=IndCQC.location_id, how="left")
    lf = lf.join(last_pir, on=IndCQC.location_id, how="left")

    return lf


def split_dataset_for_merging(lf: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Splits the dataset into two LazyFrames:
        1. Locations suitable for merging ascwds and pir.
        2. All other locations

    Locations suitable for merging are those where the last ascwds submission is
    more than two years before the last pir submission.

    Args:
        lf (pl.LazyFrame): A LazyFrame with last_ascwds_submission and last_pir_submission.

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: A tuple containing two LazyFrames:
            - Locations suitable for merging ascwds and pir.
            - All other locations.

    """
    time_expr = pl.col(IndCQC.last_ascwds_submission) < (
        pl.col(IndCQC.last_pir_submission).dt.offset_by(ThresholdValues.two_years)
    )

    lf_for_merging = lf.filter(time_expr.fill_null(False))
    lf_not_merging = lf.filter(~time_expr.fill_null(False))

    return lf_for_merging, lf_not_merging


def create_repeated_ascwds_clean_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a forward-filled version of the cleaned ASCWDS filled posts column.

    This column is needed to compare to people directly employed figures to see where they diverge.

    polars_streaming - .over() is not streaming compatible as at 13/07/2026.

    Args:
        lf (pl.LazyFrame): A LazyFrame with cleaned ascwds data

    Returns:
        pl.LazyFrame: A LazyFrame with an extra column containing ascwds filled posts filled forwards.
    """
    lf = lf.sort(IndCQC.cqc_location_import_date)

    non_nulls = lf.filter(
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean).is_not_null()
    ).select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean).alias(
            IndCQC.ascwds_filled_posts_dedup_clean_repeated
        ),
    )

    return lf.join_asof(
        non_nulls,
        on=IndCQC.cqc_location_import_date,
        by=IndCQC.location_id,
        strategy="backward",
    )


def create_ascwds_pir_merged_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column in which ascwds and pir data are merged on the following conditions:
        - The last ascwds submission is more than two years before pir submission
        - The absolute difference between ascwds and pir is above threshold
        - The percentage difference between ascwds and pir is above threshold

    The ASCWDS dataset is the preferred source for workforce filled post
    figures. However, if a workplace has not submitted ASCWDS data for a
    prolonged period of time and the corresponding PIR submission differs
    significantly (both in absolute and percentage terms) then the PIR figure is
    used instead.

    If the PIR and ASCWDS values are within acceptable difference thresholds,
    the older ASCWDS value is retained.

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame containing filled posts from
            ASCWDS and PIR and their respective submission dates.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column `ascwds_pir_merged`
            that contains either the ASCWDS or PIR filled posts value,
            depending on submission recency and similarity thresholds.
    """
    abs_diff_expr = (
        pl.col(IndCQC.pir_filled_posts_model)
        - pl.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    ).abs()

    average_of_pir_and_ascwds = (
        pl.col(IndCQC.pir_filled_posts_model)
        + pl.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    ) / 2

    condition = (abs_diff_expr > ThresholdValues.max_absolute_difference) & (
        (abs_diff_expr / average_of_pir_and_ascwds)
        > ThresholdValues.max_percentage_difference
    )

    lf = lf.with_columns(
        pl.when(condition)
        .then(pl.col(IndCQC.pir_filled_posts_model))
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_pir_merged)
    )

    return lf


def include_pir_if_never_submitted_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Copies pir_filled_posts_model values into new column 'ascwds_pir_merged'
    when ascwds_filled_posts_dedup_clean is null for all rows of same locationid.

    polars_streaming - .over() is not streaming compatible as at 13/07/2026

    Args:
        lf (pl.LazyFrame): Input LazyFrame with columns:
            - location_id
            - ascwds_filled_posts_dedup_clean
            - pir_filled_posts_model

    Returns:
        pl.LazyFrame: LazyFrame with new column 'ascwds_pir_merged'.
    """

    all_null = "all_null"
    lf_ascwds_null = lf.group_by(IndCQC.location_id).agg(
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean).is_null().all().alias(all_null)
    )

    lf = lf.join(lf_ascwds_null, on=IndCQC.location_id, how="left")

    return lf.with_columns(
        pl.when(all_null)
        .then(pl.col(IndCQC.pir_filled_posts_model))
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_pir_merged)
    ).drop(all_null)
