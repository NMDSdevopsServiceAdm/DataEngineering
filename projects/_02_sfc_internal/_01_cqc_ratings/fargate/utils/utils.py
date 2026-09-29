import hashlib

import polars as pl

from polars_utils import utils as polars_utils
from utils.column_names.cqc_ratings_columns import \
    CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.ascwds_workplace_columns import \
    AscwdsWorkplaceColumns as AWP
from utils.column_names.raw_data_files.ascwds_workplace_columns import \
    PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import \
    NewCqcLocationApiColumns as CQCL
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues, CQCRatingsValues, LocationType,
    RegistrationStatus)

# Transient column preserving raw explode order for the deterministic pivot
# tiebreak in prepare_assessment_ratings below.
EXPLODE_ORDER = "explode_order_index"

# Match Spark's `F.explode()`, which drops the row for both an empty and a null list
# (Polars only does that for empty lists by default). Unpack into every `.explode()`.
SPARK_EXPLODE = {"empty_as_null": False, "keep_nulls": False}

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
        .explode(CQCL.historic_ratings, **SPARK_EXPLODE)
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
        .explode(CQCL.assessment, **SPARK_EXPLODE)
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
        .explode(ratings_field, **SPARK_EXPLODE)
        .unnest(ratings_field)
        .explode(CQCL.key_question_ratings, **SPARK_EXPLODE)
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
    Flattens overall and ASG ratings within the assessment field into a pivoted LazyFrame.

    CQC's `keyQuestionRatings` have no upstream guarantee of exactly one entry per
    (location, assessment plan, key question). Taking each key question's rating
    ordered by `EXPLODE_ORDER` (each row's raw array position) deterministically keeps
    whichever entry appeared first in the raw feed. A conditional `group_by` aggregation
    is used instead of a pivot so the whole function stays lazy.

    Args:
        cqc_location_lf (pl.LazyFrame): Raw CQC location data, with nested
            assessments and ratings.

    Returns:
        pl.LazyFrame: One row per location's assessment plan, with key question
            ratings pivoted into individual columns (Safe, Effective, Caring,
            Responsive, Well-led).
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
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
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
        pl.LazyFrame: The same LazyFrame with unknown codes recoded to null.
    """
    return ratings_lf.with_columns(
        pl.when(~pl.col(rating_columns_to_clean).is_in(labels_to_nullify)).then(
            pl.col(rating_columns_to_clean)
        )
    )


def remove_blank_and_duplicate_rows(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Removes rows with no rating values populated at all, and de-duplicates the rest.

    This is the only full-row `unique` in the pipeline. Recoding unknown labels to
    null is what creates duplicates, so de-duplicating once after it is enough.
    """
    return ratings_lf.filter(
        pl.any_horizontal(pl.col(rating_columns_to_clean).is_not_null())
    ).unique()


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
        (
            pl.int_range(pl.len()).over(
                CQCL.location_id,
                order_by=[CQCRatings.date, CQCL.assessment_date],
                descending=True,
            )
            == 0
        )
        .cast(pl.Int32)
        .alias(CQCRatings.latest_rating_flag)
    )


def add_numerical_ratings(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds numerical rating columns for each of the key ratings and a total column.

    Ratings are matched case-insensitively; anything unrecognised (including null) is 0.
    The total covers the five key questions only, not the overall rating.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame with flattened CQC key ratings columns.

    Returns:
        pl.LazyFrame: The given LazyFrame with additional columns containing the key
            ratings as numerical values and a total of all the values.
    """
    rating_values = {
        CQCRatingsValues.outstanding.lower(): 4,
        CQCRatingsValues.good.lower(): 3,
        CQCRatingsValues.requires_improvement.lower(): 2,
        CQCRatingsValues.inadequate.lower(): 1,
    }
    rating_columns_dict = {
        CQCRatings.overall_rating: CQCRatings.overall_rating_value,
        CQCRatings.safe_rating: CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating: CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating: CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating: CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating: CQCRatings.effective_rating_value,
    }

    ratings_lf = ratings_lf.with_columns(
        pl.col(rating_column)
        .str.to_lowercase()
        .replace_strict(rating_values, default=0, return_dtype=pl.Int32)
        .alias(new_column_name)
        for rating_column, new_column_name in rating_columns_dict.items()
    )
    return ratings_lf.with_columns(
        pl.sum_horizontal(
            CQCRatings.safe_rating_value,
            CQCRatings.well_led_rating_value,
            CQCRatings.caring_rating_value,
            CQCRatings.responsive_rating_value,
            CQCRatings.effective_rating_value,
        ).alias(CQCRatings.total_rating_value)
    )


def create_standard_ratings_dataset(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Selects the standard ratings columns.

    Args:
        ratings_lf (pl.LazyFrame): A LazyFrame of CQC ratings and assessments.

    Returns:
        pl.LazyFrame: The input LazyFrame with the selected columns.
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
    )


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
        (pl.col(CQCRatings.overall_rating_value).min().over(CQCL.location_id) >= 3)
        .cast(pl.Int32)
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
