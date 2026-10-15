import polars as pl

from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import CQCRatingsValues

# Real overall ratings, worst to best, so the best rating wins any tie.
RATINGS_WORST_TO_BEST = [
    CQCRatingsValues.inadequate,
    CQCRatingsValues.requires_improvement,
    CQCRatingsValues.good,
    CQCRatingsValues.outstanding,
]


def add_date_index(
    lf: pl.LazyFrame, partition_columns: list[str], date_column: str
) -> pl.LazyFrame:
    """
    Add each row's index among its partition's distinct dates.

    A "dense" rank is used, so repeated dates share an index and no dates are skipped in the
    sequence. For example, if three rows share the same date, they all receive the same index
    value, and the next distinct date receives the next integer, such as 1, 1, 1, 2 (rather than
    1, 1, 1, 4 as standard ranking would give).

    Args:
        lf (pl.LazyFrame): dataset containing the partition and date columns
        partition_columns (list[str]): the columns identifying each timeline
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "cqc_location_import_date_indexed" added
    """
    return lf.with_columns(
        pl.col(date_column)
        .rank(method="dense")
        .over(partition_columns)
        .alias(IndCQC.cqc_location_import_date_indexed)
    )


def add_latest_overall_rating(
    lf: pl.LazyFrame, ratings_lf: pl.LazyFrame, location_column: str, date_column: str
) -> pl.LazyFrame:
    """
    Add each location's latest real overall CQC rating as of each row's date.

    Ratings are matched ignoring case. Anything that is not a real rating (blank, "Inspected
    but not rated") and undated ratings are skipped. Locations with no rating yet get "Not yet
    rated". Same-date ratings are ordered by assessment date, then the ratings job's latest
    rating flag, then the best rating. The rating is found once per location and date, then
    joined on, which keeps the memory cost small. Replaces any existing rating.

    Args:
        lf (pl.LazyFrame): dataset containing the location and date columns
        ratings_lf (pl.LazyFrame): CQC ratings dataset, with the same location column name
        location_column (str): the location ID
        date_column (str): the date each rating must be in place by

    Returns:
        pl.LazyFrame: dataset with categorical "latest_overall_rating" added
    """
    # Ratings store location IDs as strings, so match this dataset's type for the as-of join.
    location_type = lf.collect_schema()[location_column]

    lowered_rating = pl.col(CQCRatings.overall_rating).str.to_lowercase()
    real_rating = lowered_rating.replace_strict(
        {rating.lower(): rating for rating in RATINGS_WORST_TO_BEST},
        default=None,
        return_dtype=pl.String,
    )
    rating_value = lowered_rating.replace_strict(
        {rating.lower(): i for i, rating in enumerate(RATINGS_WORST_TO_BEST, start=1)},
        default=0,
    )

    ratings_by_date_lf = (
        ratings_lf.with_columns(real_rating.alias(CQCRatings.overall_rating))
        .filter(
            pl.col(CQCRatings.overall_rating).is_not_null()
            & pl.col(CQCRatings.date).is_not_null()
        )
        .group_by(location_column, CQCRatings.date)
        .agg(
            pl.col(CQCRatings.overall_rating)
            .sort_by(
                [CQCL.assessment_date, CQCRatings.latest_rating_flag, rating_value],
                descending=True,
                nulls_last=True,
            )
            .first()
            .cast(pl.Categorical)
            .alias(IndCQC.latest_overall_rating)
        )
        .with_columns(
            pl.col(location_column).cast(location_type),
            pl.col(CQCRatings.date).str.to_date("%Y-%m-%d"),
        )
        .sort(CQCRatings.date)
    )

    # join_asof needs both sides sorted by date, which Polars can't check when using `by`.
    location_date_ratings_lf = (
        lf.select(location_column, date_column)
        .unique()
        .sort(date_column)
        .join_asof(
            ratings_by_date_lf,
            left_on=date_column,
            right_on=CQCRatings.date,
            by=location_column,
            strategy="backward",
            check_sortedness=False,
        )
        .drop(CQCRatings.date)
    )

    lf = lf.drop(IndCQC.latest_overall_rating, strict=False).join(
        location_date_ratings_lf, on=[location_column, date_column], how="left"
    )

    return lf.with_columns(
        pl.col(IndCQC.latest_overall_rating).fill_null(CQCRatingsValues.not_yet_rated)
    )
