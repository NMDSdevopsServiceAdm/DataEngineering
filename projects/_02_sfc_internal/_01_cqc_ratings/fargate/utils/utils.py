import polars as pl

from polars_utils import utils as polars_utils
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import CQCCurrentOrHistoricValues

# .explode() behaivour to drop empty and null lists.
DROP_EMPTY_AND_NULL = {"empty_as_null": False, "keep_nulls": False}

KEY_QUESTION_ALIASES = {
    CQCL.safe: CQCRatings.safe_rating,
    CQCL.well_led: CQCRatings.well_led_rating,
    CQCL.caring: CQCRatings.caring_rating,
    CQCL.responsive: CQCRatings.responsive_rating,
    CQCL.effective: CQCRatings.effective_rating,
}


def keep_latest_per_key(lf: pl.LazyFrame, key_col: str, order_col: str) -> pl.LazyFrame:
    """
    Retains only the latest row for each unique key, based on a specified ordering column.

    Finds the highest `order_col` value per key from those two columns alone, then
    semi-joins back to the full frame. This avoids sorting every row (including wide
    nested columns) just to discard all but one per key. A final `unique` on the key
    guarantees one row per key if the highest `order_col` value is tied.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        key_col (str): The column name to partition by (e.g. 'location_id').
        order_col (str): The column name used to determine the latest row (e.g.
            'import_date').

    Returns:
        pl.LazyFrame: A LazyFrame containing only the latest row per key.
    """
    latest_lf = lf.group_by(key_col).agg(pl.col(order_col).max())
    return lf.join(latest_lf, on=[key_col, order_col], how="semi").unique(
        subset=[key_col]
    )


def filter_to_first_import_of_most_recent_month(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters a LazyFrame down to the earliest import date within the most recent year/month.

    Args:
        lf (pl.LazyFrame): Input LazyFrame with year, month and day partition columns.

    Returns:
        pl.LazyFrame: LazyFrame filtered to the first day imported in the latest month.
    """
    lf = polars_utils.filter_to_maximum_value_in_column(lf, Keys.year)
    lf = polars_utils.filter_to_maximum_value_in_column(lf, Keys.month)
    return lf.filter(pl.col(Keys.day) == pl.col(Keys.day).min())


def raise_on_duplicate_key_question_names(key_question_ratings: pl.Expr) -> pl.Expr:
    """
    Passes through a `keyQuestionRatings` list, raising a ValueError if any list
    repeats a name.

    Selecting by name keeps only the first match, so a repeated name would silently
    drop a rating. The check is part of the lazy plan, so the error is raised when the
    pipeline is collected rather than forcing an earlier collect.

    Args:
        key_question_ratings (pl.Expr): Expression for a `keyQuestionRatings` list of
            name/rating structs.

    Returns:
        pl.Expr: The unchanged `key_question_ratings` expression.
    """

    def _check(series: pl.Series) -> pl.Series:
        names = series.list.eval(pl.element().struct.field(CQCL.name))
        if (names.list.n_unique() != names.list.len()).any():
            raise ValueError("Duplicate key question names found in a ratings list.")
        return series

    return key_question_ratings.map_batches(_check, is_elementwise=True)


def get_key_question_rating_exprs(key_question_ratings: pl.Expr) -> list[pl.Expr]:
    """
    Builds one rating expression per key question, selected by name.

    Selecting by name means the result doesn't depend on list order. A key question
    missing from the list gives null. Raises a ValueError if any list repeats a name.

    Args:
        key_question_ratings (pl.Expr): Expression for a `keyQuestionRatings` list of
            name/rating structs.

    Returns:
        list[pl.Expr]: Rating expressions aliased to the key question rating columns.
    """
    key_question_ratings = raise_on_duplicate_key_question_names(key_question_ratings)

    return [
        key_question_ratings.list.eval(
            pl.element().filter(pl.element().struct.field(CQCL.name) == name)
        )
        .list.first()
        .struct.field(CQCL.rating)
        .alias(alias)
        for name, alias in KEY_QUESTION_ALIASES.items()
    ]


def prepare_current_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the current ratings struct into one row per location, labelled as current.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened current ratings, flagged as current.
    """
    overall = pl.col(CQCL.current_ratings).struct.field(CQCL.overall)

    return cqc_location_lf.select(
        CQCL.location_id,
        CQCL.registration_status,
        overall.struct.field(CQCL.report_date).alias(CQCRatings.date),
        overall.struct.field(CQCL.rating).alias(CQCRatings.overall_rating),
        *get_key_question_rating_exprs(overall.struct.field(CQCL.key_question_ratings)),
        pl.lit(CQCCurrentOrHistoricValues.current).alias(
            CQCRatings.current_or_historic
        ),
    )


def prepare_historic_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the historic ratings list into one row per location historic rating entry.

    Explodes `historicRatings`. Every entry stays its own row, so entries sharing a
    location and report date are kept rather than collapsed. This avoids a pivot, so
    the whole function stays lazy.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened historic ratings, flagged as historic.
    """
    key_question_ratings = (
        pl.col(CQCL.historic_ratings)
        .struct.field(CQCL.overall)
        .struct.field(CQCL.key_question_ratings)
    )

    return (
        cqc_location_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.historic_ratings,
        )
        .explode(CQCL.historic_ratings, **DROP_EMPTY_AND_NULL)
        .select(
            CQCL.location_id,
            CQCL.registration_status,
            pl.col(CQCL.historic_ratings)
            .struct.field(CQCL.report_date)
            .alias(CQCRatings.date),
            pl.col(CQCL.historic_ratings)
            .struct.field(CQCL.overall)
            .struct.field(CQCL.rating)
            .alias(CQCRatings.overall_rating),
            *get_key_question_rating_exprs(key_question_ratings),
            pl.lit(CQCCurrentOrHistoricValues.historic).alias(
                CQCRatings.current_or_historic
            ),
        )
    )
