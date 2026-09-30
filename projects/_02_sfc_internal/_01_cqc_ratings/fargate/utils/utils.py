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


def prepare_current_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the current ratings struct into one row per location, labelled as current.

    The five key questions are picked out of `keyQuestionRatings` by fixed position
    (Safe, Well-led, Caring, Responsive, Effective). `null_on_oob=True` handles
    locations with fewer than 5 key questions.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened current ratings, flagged as current.
    """
    overall = pl.col(CQCL.current_ratings).struct.field(CQCL.overall)
    key_question_ratings = overall.struct.field(CQCL.key_question_ratings)
    key_question_aliases = [
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
    ]

    return cqc_location_lf.select(
        CQCL.location_id,
        CQCL.registration_status,
        overall.struct.field(CQCL.report_date).alias(CQCRatings.date),
        overall.struct.field(CQCL.rating).alias(CQCRatings.overall_rating),
        *[
            key_question_ratings.list.get(position, null_on_oob=True)
            .struct.field(CQCL.rating)
            .alias(alias)
            for position, alias in enumerate(key_question_aliases)
        ],
        pl.lit(CQCCurrentOrHistoricValues.current).alias(
            CQCRatings.current_or_historic
        ),
    )


def prepare_historic_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the historic ratings list into one row per location historic rating entry.

    Explodes `historicRatings`, then picks each key question's rating out of the
    entry's `keyQuestionRatings` list by name. Every entry stays its own row, so
    entries sharing a location and report date are kept rather than collapsed. This
    avoids a pivot, so the whole function stays lazy.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened historic ratings, flagged as historic.
    """
    key_question_aliases = {
        CQCL.safe: CQCRatings.safe_rating,
        CQCL.well_led: CQCRatings.well_led_rating,
        CQCL.caring: CQCRatings.caring_rating,
        CQCL.responsive: CQCRatings.responsive_rating,
        CQCL.effective: CQCRatings.effective_rating,
    }
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
            *[
                key_question_ratings.list.eval(
                    pl.element().filter(pl.element().struct.field(CQCL.name) == name)
                )
                .list.first()
                .struct.field(CQCL.rating)
                .alias(alias)
                for name, alias in key_question_aliases.items()
            ],
            pl.lit(CQCCurrentOrHistoricValues.historic).alias(
                CQCRatings.current_or_historic
            ),
        )
    )
