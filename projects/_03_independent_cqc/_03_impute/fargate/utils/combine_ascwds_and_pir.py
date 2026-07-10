import polars as pl

from polars_utils.expressions import is_not_care_home
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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
        lf (pl.LazyFrame): Input PySpark LazyFrame containing filled posts from ASCWDS and PIR and their respective submission dates.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column `ascwds_pir_merged` that contains either the ASCWDS or PIR filled posts value,
                      depending on submission recency and similarity thresholds.
    """
    lf = create_repeated_ascwds_clean_column(lf)
    lf = create_last_submission_columns(lf)
    lf = create_ascwds_pir_merged_column(lf)
    lf = include_pir_if_never_submitted_ascwds(lf)
    lf = drop_temporary_columns(lf)
    return lf


def create_repeated_ascwds_clean_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a column containing cleaned ascwds filled posts filled forwards.

    This column is needed to compare to people directly employed figures to see where they diverge.

    Args:
        lf (pl.LazyFrame): A LazyFrame with cleaned ascwds data

    Returns:
        pl.LazyFrame: A LazyFrame with an extra column containing ascwds filled posts filled forwards.
    """
    lf = lf.with_columns(
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
        .forward_fill()
        .over(IndCQC.location_id, order_by=IndCQC.cqc_location_import_date)
        .alias(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    )
    return lf


def create_last_submission_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds columns with the max cqc_location_import_date per location_id for
    ascwds and pir data.

    This column is needed to identify whether there has been a gap of at least two years
    between an ascwds submission and a pir submission.

    Args:
        lf (pl.LazyFrame): A LazyFrame with ascwds and pir data.

    Returns:
        pl.LazyFrame: A LazyFrame with two extra columns containing the latest submission dates.
    """
    lf = lf.with_columns(
        pl.col(IndCQC.cqc_location_import_date)
        .filter(pl.col(IndCQC.ascwds_filled_posts_dedup_clean).is_not_null())
        .max()
        .over(IndCQC.location_id)
        .alias(IndCQC.last_ascwds_submission),
        pl.col(IndCQC.cqc_location_import_date)
        .filter(pl.col(IndCQC.pir_filled_posts_model).is_not_null())
        .max()
        .over(IndCQC.location_id)
        .alias(IndCQC.last_pir_submission),
    )

    return lf


def create_ascwds_pir_merged_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merges ASCWDS and PIR filled post estimates based on recently and similarity thresholds and stores in a new column.

    The ASCWDS dataset is the preferred source for workforce filled post figures.
    However, if a workplace has not submitted ASCWDS data for a prolonged period of time and the corresponding PIR
    submission differs significantly (both in absolute and percentage terms) then the PIR figure is used instead.

    If the PIR and ASCWDS values are within acceptable difference thresholds, the older ASCWDS value is retained.

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame containing filled posts from ASCWDS and PIR and their respective submission dates.

    Returns:
        pl.LazyFrame: A LazyFrame with an additional column `ascwds_pir_merged` that contains either the ASCWDS or PIR filled posts value,
                      depending on submission recency and similarity thresholds.
    """
    # time_between_last_pir_and_ascwds_submissions = F.months_between(
    #     F.col(IndCQC.last_pir_submission),
    #     F.col(IndCQC.last_ascwds_submission),
    # )
    # absolute_difference_between_pir_and_ascwds = F.abs(
    #     F.col(IndCQC.pir_filled_posts_model)
    #     - F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    # )
    # average_of_pir_and_ascwds = (
    #     F.col(IndCQC.pir_filled_posts_model)
    #     + F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    # ) / 2

    # lf = lf.withColumn(
    #     IndCQC.ascwds_pir_merged,
    #     F.when(
    #         (
    #             time_between_last_pir_and_ascwds_submissions
    #             > ThresholdValues.months_in_two_years
    #         )
    #         & (
    #             absolute_difference_between_pir_and_ascwds
    #             > ThresholdValues.max_absolute_difference
    #         )
    #         & (
    #             (absolute_difference_between_pir_and_ascwds / average_of_pir_and_ascwds)
    #             > ThresholdValues.max_percentage_difference
    #         ),
    #         F.col(IndCQC.pir_filled_posts_model),
    #     ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    # )
    return lf


def include_pir_if_never_submitted_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Updates the 'ascwds_pir_merged' column to use PIR filled posts model values for locations who have never submitted ASC-WDS data.

    Args:
        lf (pl.LazyFrame): Input LazyFrame with columns:
            - location_id (str)
            - ascwds_pir_merged (double)
            - pir_filled_posts_model (double)

    Returns:
        pl.LazyFrame: LazyFrame with updated 'ascwds_pir_merged' values
                   where applicable.
    """
    # w = Window.partitionBy(IndCQC.location_id)

    # lf = lf.withColumn(
    #     IndCQC.submitted_ascwds_data,
    #     F.max(F.col(IndCQC.ascwds_pir_merged).isNotNull().cast(IntegerType())).over(w),
    # )
    # lf = lf.withColumn(
    #     IndCQC.ascwds_pir_merged,
    #     F.when(
    #         F.col(IndCQC.submitted_ascwds_data) == 0,
    #         F.col(IndCQC.pir_filled_posts_model),
    #     ).otherwise(F.col(IndCQC.ascwds_pir_merged)),
    # ).drop(IndCQC.submitted_ascwds_data)

    return lf


def drop_temporary_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Drops temporary columns from the blend pir and ascwds function.

    Args:
        lf (pl.LazyFrame): A LazyFrame which has just had the ascwds and pir data blended.

    Returns:
        pl.LazyFrame: A LazyFrame with temporary columns removed.
    """
    lf = lf.drop(
        IndCQC.last_ascwds_submission,
        IndCQC.last_pir_submission,
        IndCQC.ascwds_filled_posts_dedup_clean_repeated,
    )
    return lf
