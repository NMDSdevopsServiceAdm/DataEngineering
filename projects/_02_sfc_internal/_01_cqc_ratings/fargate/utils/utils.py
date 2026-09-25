import hashlib

import polars as pl

from polars_utils import utils as polars_utils
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    LocationType,
    RegistrationStatus,
)

# Transient column preserving raw explode order for the deterministic pivot
# tiebreak in prepare_assessment_ratings below.
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

rating_columns_to_clean = [
    CQCRatings.overall_rating,
    CQCRatings.safe_rating,
    CQCRatings.well_led_rating,
    CQCRatings.caring_rating,
    CQCRatings.responsive_rating,
    CQCRatings.effective_rating,
]

labels_to_nullify = [
    "Inspected but not rated",
    "No published rating",
    "Insufficient evidence to rate",
    "Evidence requirements partially / not yet scored",
    "No Approved Rating",
    "",
]


def keep_latest_per_key(lf: pl.LazyFrame, key_col: str, order_col: str) -> pl.LazyFrame:
    """
    Retains only the latest row for each unique key, based on a specified ordering column.

    Sorts descending by `order_col` and keeps the first row seen per `key_col`, i.e.
    the row with the highest `order_col` value per key.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        key_col (str): The column name to partition by (e.g. 'location_id').
        order_col (str): The column name used to determine the latest row (e.g.
            'import_date').

    Returns:
        pl.LazyFrame: A LazyFrame containing only the latest row per key.
    """
    return lf.sort(order_col, descending=True).unique(subset=[key_col], keep="first")


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
    lf = lf.with_columns(pl.col(Keys.day).min().alias("min_day")).filter(
        pl.col(Keys.day) == pl.col("min_day")
    )
    return lf.drop("min_day")


def prepare_current_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the current ratings struct into one row per location, recoded and labelled.

    The five key questions are picked out of `keyQuestionRatings` by fixed position
    (Safe, Well-led, Caring, Responsive, Effective). `null_on_oob=True` handles
    locations with fewer than 5 key questions.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened current ratings, recoded and flagged as current.
    """
    overall = pl.col(CQCL.current_ratings).struct.field(CQCL.overall)
    key_question_ratings = overall.struct.field(CQCL.key_question_ratings)
    # Position in keyQuestionRatings -> target column, per the docstring above.
    key_question_aliases = [
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
    ]

    current_ratings_lf = cqc_location_lf.select(
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
    )
    current_ratings_lf = recode_unknown_codes_to_null(current_ratings_lf)
    current_ratings_lf = add_current_or_historic_column(
        current_ratings_lf, CQCCurrentOrHistoricValues.current
    )
    return current_ratings_lf


def prepare_historic_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the historic ratings list into one row per location/report date/key question.

    Explodes `historicRatings` and each entry's `keyQuestionRatings`, then pivots so
    each key question becomes its own column. `.pivot()` is eager-only in Polars,
    hence the `.collect()` - the result is small (one row per location/report date),
    so this is an acceptable aggregation-sized collect.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data.

    Returns:
        pl.LazyFrame: Flattened historic ratings, recoded and flagged as historic.
    """
    exploded_lf = (
        cqc_location_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.historic_ratings,
        )
        .explode(CQCL.historic_ratings, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.historic_ratings)
            .struct.field(CQCL.report_date)
            .alias(CQCRatings.date),
            pl.col(CQCL.historic_ratings)
            .struct.field(CQCL.overall)
            .struct.field(CQCL.rating)
            .alias(CQCRatings.overall_rating),
            pl.col(CQCL.historic_ratings)
            .struct.field(CQCL.overall)
            .struct.field(CQCL.key_question_ratings)
            .alias(CQCL.key_question_ratings),
        )
        .drop(CQCL.historic_ratings)
        .explode(CQCL.key_question_ratings, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.key_question_ratings).struct.field(CQCL.name).alias(CQCL.name),
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.rating)
            .alias(CQCL.rating),
        )
        .drop(CQCL.key_question_ratings)
    )

    grain_columns = [
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCRatings.overall_rating,
    ]

    historic_ratings_df = exploded_lf.collect().pivot(
        on=CQCL.name,
        index=grain_columns,
        values=CQCL.rating,
        aggregate_function="first",
    )
    historic_ratings_df = historic_ratings_df.rename(
        {
            CQCL.safe: CQCRatings.safe_rating,
            CQCL.well_led: CQCRatings.well_led_rating,
            CQCL.caring: CQCRatings.caring_rating,
            CQCL.responsive: CQCRatings.responsive_rating,
            CQCL.effective: CQCRatings.effective_rating,
        }
    )

    historic_ratings_lf = recode_unknown_codes_to_null(historic_ratings_df.lazy())
    historic_ratings_lf = add_current_or_historic_column(
        historic_ratings_lf, CQCCurrentOrHistoricValues.historic
    )
    return historic_ratings_lf


def extract_assessment_base(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Explodes the raw `assessment` list so each row is one assessment plan.

    Every `.explode()` in this module passes `empty_as_null=False, keep_nulls=False`
    to match Spark's `F.explode()`, which drops the row for both an empty and a null
    list (Polars only does that for empty lists by default).

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
        .explode(CQCL.assessment, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.assessment)
            .struct.field(CQCL.assessment_plan_published_datetime)
            .alias(CQCL.assessment_plan_published_datetime),
            pl.col(CQCL.assessment)
            .struct.field(CQCL.ratings)
            .alias(CQCL.assessments_ratings),
        )
        .drop(CQCL.assessment)
    )


def extract_overall(assessment_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the 'overall' ratings out of the assessment ratings struct.

    Args:
        assessment_lf (pl.LazyFrame): Output of `extract_assessment_base`.

    Returns:
        pl.LazyFrame: One row per location/assessment plan/key question, with an
            explode-order index column preserving raw array order.
    """
    return (
        assessment_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.assessment_plan_published_datetime,
            pl.col(CQCL.assessments_ratings)
            .struct.field(CQCL.overall)
            .alias(CQCL.overall),
        )
        .explode(CQCL.overall, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.overall).struct.field(CQCL.rating).alias(CQCL.rating),
            pl.col(CQCL.overall).struct.field(CQCL.status).alias(CQCL.status),
            pl.col(CQCL.overall)
            .struct.field(CQCL.key_question_ratings)
            .alias(CQCL.key_question_ratings),
        )
        .drop(CQCL.overall)
        .explode(CQCL.key_question_ratings, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.name)
            .alias(CQCL.key_question_name),
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.rating)
            .alias(CQCL.key_question_rating),
            pl.lit("SAF").alias(CQCL.dataset),
            pl.lit("assessment.ratings.overall").alias(CQCL.source_path),
        )
        .drop(CQCL.key_question_ratings)
        .with_row_index(EXPLODE_ORDER)
    )


def extract_asg(assessment_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens the 'asgRatings' (service-level) ratings out of the assessment ratings struct.

    Args:
        assessment_lf (pl.LazyFrame): Output of `extract_assessment_base`.

    Returns:
        pl.LazyFrame: One row per location/assessment plan/key question, with an
            explode-order index column preserving raw array order.
    """
    return (
        assessment_lf.select(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.assessment_plan_published_datetime,
            pl.col(CQCL.assessments_ratings)
            .struct.field(CQCL.asg_ratings)
            .alias(CQCL.asg_ratings),
        )
        .explode(CQCL.asg_ratings, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.asg_ratings)
            .struct.field(CQCL.assessment_plan_id)
            .alias(CQCL.assessment_plan_id),
            pl.col(CQCL.asg_ratings).struct.field(CQCL.title).alias(CQCL.title),
            pl.col(CQCL.asg_ratings)
            .struct.field(CQCL.assessment_date)
            .alias(CQCL.assessment_date),
            pl.col(CQCL.asg_ratings)
            .struct.field(CQCL.assessment_plan_status)
            .alias(CQCL.assessment_plan_status),
            pl.col(CQCL.asg_ratings).struct.field(CQCL.name).alias(CQCL.name),
            pl.col(CQCL.asg_ratings).struct.field(CQCL.rating).alias(CQCL.rating),
            pl.col(CQCL.asg_ratings).struct.field(CQCL.status).alias(CQCL.status),
            pl.col(CQCL.asg_ratings)
            .struct.field(CQCL.key_question_ratings)
            .alias(CQCL.key_question_ratings),
        )
        .drop(CQCL.asg_ratings)
        .explode(CQCL.key_question_ratings, empty_as_null=False, keep_nulls=False)
        .with_columns(
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.name)
            .alias(CQCL.key_question_name),
            pl.col(CQCL.key_question_ratings)
            .struct.field(CQCL.rating)
            .alias(CQCL.key_question_rating),
            pl.lit("SAF").alias(CQCL.dataset),
            pl.lit("assessment.ratings.asg_ratings").alias(CQCL.source_path),
        )
        .drop(CQCL.key_question_ratings)
        .with_row_index(EXPLODE_ORDER)
    )


def prepare_assessment_ratings(cqc_location_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Flattens overall and ASG ratings within the assessment field into a pivoted LazyFrame.

    CQC's `keyQuestionRatings` have no upstream guarantee of exactly one entry per
    (location, assessment plan, key question). Sorting by grain + `EXPLODE_ORDER`
    (each row's raw array position) before the pivot makes `aggregate_function="first"`
    deterministically keep whichever entry appeared first in the raw feed.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data, with nested
            assessments and ratings.

    Returns:
        pl.LazyFrame: One row per location's assessment plan, with key question
            ratings pivoted into individual columns (Safe, Effective, Caring,
            Responsive, Well-led).
    """
    assessment_lf = extract_assessment_base(cqc_location_lf)
    overall_lf = extract_overall(assessment_lf)
    asg_lf = extract_asg(assessment_lf)

    union_df = (
        pl.concat([overall_lf, asg_lf], how="diagonal_relaxed")
        .sort([*assessment_grain_columns, EXPLODE_ORDER])
        .collect()
    )

    assessment_ratings_df = union_df.pivot(
        on=CQCL.key_question_name,
        index=assessment_grain_columns,
        values=CQCL.key_question_rating,
        aggregate_function="first",
    ).sort(CQCL.assessment_date)

    desired_column_order = [
        *assessment_grain_columns,
        CQCL.safe,
        CQCL.effective,
        CQCL.caring,
        CQCL.responsive,
        CQCL.well_led,
    ]

    return assessment_ratings_df.select(desired_column_order).lazy()


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


def merge_cqc_ratings(
    assessment_ratings_lf: pl.LazyFrame,
    standard_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Merges assessment_ratings_lf and standard_ratings_lf to get final ratings.

    Args:
        assessment_ratings_lf (pl.LazyFrame): Output of `prepare_assessment_ratings`,
            containing flattened assessment ratings.
        standard_ratings_lf (pl.LazyFrame): Flattened current/historic CQC ratings.

    Returns:
        pl.LazyFrame: Merged LazyFrame of pre-SAF CQC ratings and the new assessment
            ASG ratings.
    """
    expected_columns = [
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        CQCRatings.current_or_historic,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
    ]

    standard_lf = standard_ratings_lf.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCRatings.current_or_historic,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
        pl.lit("Pre SAF").alias(CQCL.dataset),
    )
    assessment_lf = assessment_ratings_lf.select(
        CQCL.location_id,
        CQCL.registration_status,
        pl.col(CQCL.assessment_plan_published_datetime)
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
        .cast(pl.Date)
        .alias(CQCRatings.date),
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        pl.col(CQCL.status).alias(CQCRatings.current_or_historic),
        pl.col(CQCL.rating).alias(CQCRatings.overall_rating),
        pl.col(CQCL.safe).alias(CQCRatings.safe_rating),
        pl.col(CQCL.well_led).alias(CQCRatings.well_led_rating),
        pl.col(CQCL.caring).alias(CQCRatings.caring_rating),
        pl.col(CQCL.responsive).alias(CQCRatings.responsive_rating),
        pl.col(CQCL.effective).alias(CQCRatings.effective_rating),
    )
    merged_lf = pl.concat([standard_lf, assessment_lf], how="diagonal_relaxed")
    return merged_lf.select(expected_columns)


def recode_unknown_codes_to_null(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Recodes non-rating labels (e.g. "No published rating") in the rating columns to null.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame of CQC ratings.

    Returns:
        pl.LazyFrame: The same LazyFrame with unknown codes recoded to null and
            duplicate rows removed.
    """
    ratings_lf = ratings_lf.with_columns(
        [
            pl.when(pl.col(col_name).is_in(labels_to_nullify))
            .then(None)
            .otherwise(pl.col(col_name))
            .alias(col_name)
            for col_name in rating_columns_to_clean
        ]
    )
    return ratings_lf.unique()


def add_current_or_historic_column(
    ratings_lf: pl.LazyFrame, current_or_historic: str
) -> pl.LazyFrame:
    """Adds a literal column flagging rows as current or historic ratings."""
    return ratings_lf.with_columns(
        pl.lit(current_or_historic).alias(CQCRatings.current_or_historic)
    )


def remove_blank_and_duplicate_rows(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Removes rows with no rating values populated at all, and de-duplicates the rest."""
    return ratings_lf.filter(
        pl.col(CQCRatings.overall_rating).is_not_null()
        | pl.col(CQCRatings.safe_rating).is_not_null()
        | pl.col(CQCRatings.well_led_rating).is_not_null()
        | pl.col(CQCRatings.caring_rating).is_not_null()
        | pl.col(CQCRatings.responsive_rating).is_not_null()
        | pl.col(CQCRatings.effective_rating).is_not_null()
    ).unique()


def add_rating_sequence_column(
    ratings_lf: pl.LazyFrame, reversed: bool = False
) -> pl.LazyFrame:
    """
    Adds a column with the ratings sequenced by publication date and assessment date.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame of CQC ratings and assessments.
        reversed (bool): Whether to sequence oldest to newest (False) or newest to
            oldest (True). Defaults to False.

    Returns:
        pl.LazyFrame: The input LazyFrame with a column showing the desired sequence.
    """
    order_by = [pl.col(CQCRatings.date), pl.col(CQCL.assessment_date)]
    new_column_name = (
        CQCRatings.reversed_rating_sequence if reversed else CQCRatings.rating_sequence
    )
    return ratings_lf.with_columns(
        pl.int_range(1, pl.len() + 1)
        .over(CQCL.location_id, order_by=order_by, descending=reversed)
        .alias(new_column_name)
    )


def add_latest_rating_flag_column(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column to flag the latest rating per location_id as 1, otherwise 0.

    The latest rating is defined as the first row per location_id, when sorted in
    descending order on rating_date and assessment_date. A location may have multiple
    ratings on the same date and be its latest date. In these cases the flag is random
    between the group.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame with flattened CQC key ratings columns.

    Returns:
        pl.LazyFrame: The given LazyFrame with an additional column to flag the latest rating.
    """
    return ratings_lf.with_columns(
        pl.when(pl.col(CQCRatings.reversed_rating_sequence) == 1)
        .then(1)
        .otherwise(0)
        .alias(CQCRatings.latest_rating_flag)
    )


def add_numerical_ratings(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds numerical rating columns for each of the key ratings and a total column.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame with flattened CQC key ratings columns.

    Returns:
        pl.LazyFrame: The given LazyFrame with additional columns containing the key
            ratings as numerical values and a total of all the values.
    """
    rating_columns_dict = {
        CQCRatings.overall_rating: CQCRatings.overall_rating_value,
        CQCRatings.safe_rating: CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating: CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating: CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating: CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating: CQCRatings.effective_rating_value,
    }

    ratings_lf = ratings_lf.with_columns(
        [
            pl.when(
                pl.col(rating_column).str.to_lowercase()
                == CQCRatingsValues.outstanding.lower()
            )
            .then(4)
            .when(
                pl.col(rating_column).str.to_lowercase()
                == CQCRatingsValues.good.lower()
            )
            .then(3)
            .when(
                pl.col(rating_column).str.to_lowercase()
                == CQCRatingsValues.requires_improvement.lower()
            )
            .then(2)
            .when(
                pl.col(rating_column).str.to_lowercase()
                == CQCRatingsValues.inadequate.lower()
            )
            .then(1)
            .otherwise(0)
            .alias(new_column_name)
            for rating_column, new_column_name in rating_columns_dict.items()
        ]
    )
    return ratings_lf.with_columns(
        (
            pl.col(CQCRatings.safe_rating_value)
            + pl.col(CQCRatings.well_led_rating_value)
            + pl.col(CQCRatings.caring_rating_value)
            + pl.col(CQCRatings.responsive_rating_value)
            + pl.col(CQCRatings.effective_rating_value)
        ).alias(CQCRatings.total_rating_value)
    )


def create_standard_ratings_dataset(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Selects the standard ratings columns and removes duplicate rows.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame of CQC ratings and assessments.

    Returns:
        pl.LazyFrame: The input LazyFrame with selected columns and duplicate rows removed.
    """
    return ratings_lf.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        CQCRatings.latest_rating_flag,
        CQCRatings.current_or_historic,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
        CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating_value,
        CQCRatings.total_rating_value,
    ).unique()


def add_location_id_hash(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column with a 20 character hashed version of the location ID.

    This hash is used for linking with anonymised files. Polars has no native SHA-256
    expression, so this uses `.map_elements()` with the stdlib `hashlib` - the one
    exception to avoiding `.map_elements()` in this repo, since no vectorised
    alternative exists.

    Args:
        ratings_lf (pl.LazyFrame): A prepared standard ratings LazyFrame containing
            the column location_id.

    Returns:
        pl.LazyFrame: The same LazyFrame with an additional column containing the
            hashed location id.
    """
    return ratings_lf.with_columns(
        pl.col(CQCL.location_id)
        .map_elements(
            lambda location_id: hashlib.sha256(location_id.encode()).hexdigest()[:20],
            return_dtype=pl.String,
        )
        .alias(CQCRatings.location_id_hash)
    )


def select_ratings_for_benchmarks(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters to rows which are registered and current rating only.

    Args:
        ratings_lf (pl.LazyFrame): A prepared standard ratings LazyFrame containing the
            columns registration_status, current_or_historic and latest_rating_flag.

    Returns:
        pl.LazyFrame: A LazyFrame filtered to registered and current rating only.
    """
    return ratings_lf.filter(
        (pl.col(CQCL.registration_status) == RegistrationStatus.registered)
        & (pl.col(CQCRatings.current_or_historic) == CQCCurrentOrHistoricValues.current)
    )


def add_good_and_outstanding_flag_column(
    benchmark_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Flags locations where the minimum overall rating value is 3 (good).

    Args:
        benchmark_ratings_lf (pl.LazyFrame): A LazyFrame filtered to registered and
            current rating only.

    Returns:
        pl.LazyFrame: The input LazyFrame with a column to flag locations with good
            and outstanding current ratings.
    """
    return benchmark_ratings_lf.with_columns(
        pl.when(
            pl.col(CQCRatings.overall_rating_value).min().over(CQCL.location_id) >= 3
        )
        .then(1)
        .otherwise(0)
        .alias(CQCRatings.good_or_outstanding_flag)
    )


def join_establishment_ids(
    benchmark_ratings_lf: pl.LazyFrame, ascwds_workplace_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Joins ASC-WDS establishment IDs onto the benchmark ratings data by location_id."""
    ascwds_workplace_lf = ascwds_workplace_lf.select(
        pl.col(AWP.location_id).alias(CQCL.location_id),
        AWP.establishment_id,
    )
    return benchmark_ratings_lf.join(
        ascwds_workplace_lf, on=CQCL.location_id, how="left"
    )


def create_benchmark_ratings_dataset(
    benchmark_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Selects and renames the benchmark ratings columns, filtering to complete rows."""
    benchmark_ratings_lf = benchmark_ratings_lf.select(
        pl.col(CQCL.location_id).alias(CQCRatings.benchmarks_location_id),
        pl.col(AWP.establishment_id).alias(CQCRatings.benchmarks_establishment_id),
        CQCL.name,
        CQCL.dataset,
        CQCRatings.good_or_outstanding_flag,
        pl.col(CQCRatings.overall_rating).alias(CQCRatings.benchmarks_overall_rating),
        pl.col(CQCRatings.date).alias(CQCRatings.inspection_date),
    )
    return benchmark_ratings_lf.filter(
        pl.col(CQCRatings.benchmarks_establishment_id).is_not_null()
        & pl.col(CQCRatings.benchmarks_overall_rating).is_not_null()
    )
