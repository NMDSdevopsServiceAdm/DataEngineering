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
drop_empty_and_null = {"empty_as_null": False, "keep_nulls": False}

# Raw array position of each exploded row, so duplicate key question entries are
# resolved deterministically.
EXPLODE_ORDER = "explode_order_index"

assessment_grain_columns = [
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.assessment_plan_published_datetime,
    CQCL.assessment_plan_id,
    CQCL.title,
    CQCL.assessment_date,
    CQCL.assessment_plan_status,
    CQCL.dataset,
    CQCL.name,
    CQCL.status,
    CQCL.rating,
    CQCL.source_path,
]


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
        .explode(CQCL.historic_ratings, **drop_empty_and_null)
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


def extract_assessment_base(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Explodes the raw `assessment` list so each row is one assessment plan.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: One row per location per assessment plan, with the nested
            ratings struct carried through unflattened.
    """
    return (
        cqc_location_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.assessment,
        )
        .explode(CQCL.assessment, **drop_empty_and_null)
        .select(
            CQCL.location_id,
            CQCL.registration_status,
            pl.col(CQCL.assessment).struct.field(
                CQCL.assessment_plan_published_datetime
            ),
            pl.col(CQCL.assessment)
            .struct.field(CQCL.ratings)
            .alias(CQCL.assessments_ratings),
        )
    )


def extract_key_question_ratings(
    assessment_lf: pl.LazyFrame, ratings_field: str, source_path: str
) -> pl.LazyFrame:
    """
    Flattens one ratings list of the assessment ratings struct to one row per key question.

    Used for both the `overall` and `asgRatings` (service-level) lists. Fields that
    only exist in one of them (e.g. `assessment_plan_id`) are simply absent from the
    other, and are aligned when the two are concatenated.

    Args:
        assessment_lf (pl.LazyFrame): Output of `extract_assessment_base`.
        ratings_field (str): The list within the ratings struct to flatten
            (`CQCL.overall` or `CQCL.asg_ratings`).
        source_path (str): Value for the `source_path` column recording where the
            ratings came from.

    Returns:
        pl.LazyFrame: One row per location/assessment plan/key question, with an
            explode-order index column preserving raw array order.
    """
    return (
        assessment_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.assessment_plan_published_datetime,
            pl.col(CQCL.assessments_ratings).struct.field(ratings_field),
        )
        .explode(ratings_field, **drop_empty_and_null)
        .unnest(ratings_field)
        .explode(CQCL.key_question_ratings, **drop_empty_and_null)
        .with_columns(
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.name)
            .alias(CQCL.key_question_name),
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.rating)
            .alias(CQCL.key_question_rating),
            pl.lit("SAF").alias(CQCL.dataset),
            pl.lit(source_path).alias(CQCL.source_path),
        )
        .drop(CQCL.key_question_ratings)
        .with_row_index(EXPLODE_ORDER)
    )


def prepare_assessment_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens overall and ASG ratings within the assessment field, one column per key question.

    CQC's `keyQuestionRatings` have no upstream guarantee of exactly one entry per
    (location, assessment plan, key question). Taking each key question's rating
    ordered by `EXPLODE_ORDER` (each row's raw array position) deterministically keeps
    whichever entry appeared first in the raw feed. A conditional `group_by` aggregation
    is used instead of a pivot so the whole function stays lazy.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data, with nested
            assessments and ratings.

    Returns:
        pl.LazyFrame: One row per location's assessment plan, with one column per
            key question rating (Safe, Effective, Caring, Responsive, Well-led).
    """
    assessment_lf = extract_assessment_base(cqc_location_lf)
    overall_lf = extract_key_question_ratings(
        assessment_lf, CQCL.overall, "assessment.ratings.overall"
    )
    asg_lf = extract_key_question_ratings(
        assessment_lf, CQCL.asg_ratings, "assessment.ratings.asg_ratings"
    )

    key_question_names = [
        CQCL.safe,
        CQCL.effective,
        CQCL.caring,
        CQCL.responsive,
        CQCL.well_led,
    ]

    return (
        pl.concat([overall_lf, asg_lf], how="diagonal_relaxed")
        .group_by(assessment_grain_columns)
        .agg(
            pl.col(CQCL.key_question_rating)
            .filter(pl.col(CQCL.key_question_name) == name)
            .sort_by(
                pl.col(EXPLODE_ORDER).filter(pl.col(CQCL.key_question_name) == name)
            )
            .first()
            .alias(name)
            for name in key_question_names
        )
    )


def raise_error_when_assessment_df_contains_overall_data(
    assessment_ratings_lf: pl.LazyFrame,
) -> None:
    """
    Raise an error when the assessments LazyFrame contains any overall ratings data.

    Currently, CQC publish an overall rating object within the assessments column, but
    this is not populated for any social care locations we've checked as at 15/09/2025.
    It is published for non-social care locations.
    This overall rating object can have its own "rating" and "key question ratings".

    CQC also publish a "rating" and "key question ratings" within the asg_ratings object
    within the assessments column. This "rating" and "key question ratings" are at the
    service level within a location (one location can have many services). We are
    referring to this "rating" as the overall rating for social care locations.

    This function raises a value error if the overall object contains any values.
    If this happens, we need to refactor the flattening of CQC assessments data.

    Args:
        assessment_ratings_lf (pl.LazyFrame): LazyFrame of flattened CQC assessments data.

    Raises:
        ValueError: If the LazyFrame contains overall assessments data.
    """
    rows_where_overall_has_value = (
        assessment_ratings_lf.filter(
            (pl.col(CQCL.source_path) == "assessment.ratings.overall")
            & pl.col(CQCL.rating).is_not_null()
        )
        .select(pl.len())
        .collect()
        .item()
    )

    if rows_where_overall_has_value > 0:
        raise ValueError(
            f"The overall object within the assessments column contains {rows_where_overall_has_value} values for social care locations."
        )

    return None
