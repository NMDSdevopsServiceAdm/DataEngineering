import hashlib
import sys

import polars as pl

from polars_utils import utils
from projects._02_sfc_internal._01_cqc_ratings.fargate.utils import utils as rUtils
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
    LocationType,
    RegistrationStatus,
)

DELTA_COLUMNS_TO_IMPORT = [
    CQCL.location_id,
    Keys.import_date,
    CQCL.current_ratings,
    CQCL.historic_ratings,
    CQCL.assessment,
]

SNAPSHOT_COLUMNS_TO_IMPORT = [
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.type,
]

ASCWDS_WORKPLACE_COLUMNS_TO_IMPORT = [
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
    AWP.establishment_id,
    AWP.location_id,
]


def main(
    cqc_full_snapshot_source: str,
    cqc_locations_api_delta_source: str,
    ascwds_workplace_source: str,
    cqc_ratings_destination: str,
    benchmark_ratings_destination: str,
) -> None:
    """Flattens CQC ratings and assessments into standard and benchmark datasets.

    Args:
        cqc_full_snapshot_source (str): Source s3 directory for the latest full
            snapshot of CQC registered and deregistered locations.
        cqc_locations_api_delta_source (str): Source s3 directory for raw CQC
            locations delta data.
        ascwds_workplace_source (str): Source s3 directory for ASC-WDS workplace
            data.
        cqc_ratings_destination (str): Destination s3 directory for the standard CQC
            ratings dataset.
        benchmark_ratings_destination (str): Destination s3 directory for the
            benchmark ratings dataset.
    """
    cqc_snapshot_lf = utils.scan_parquet(
        cqc_full_snapshot_source, selected_columns=SNAPSHOT_COLUMNS_TO_IMPORT
    )
    cqc_snapshot_lf = cqc_snapshot_lf.filter(
        pl.col(CQCL.type) == LocationType.social_care_identifier
    )

    cqc_delta_lf = utils.scan_parquet(
        cqc_locations_api_delta_source, selected_columns=DELTA_COLUMNS_TO_IMPORT
    )
    cqc_delta_lf = rUtils.keep_latest_per_key(
        cqc_delta_lf, CQCL.location_id, Keys.import_date
    )

    cqc_ratings_lf = cqc_snapshot_lf.join(cqc_delta_lf, on=CQCL.location_id, how="left")

    ascwds_workplace_lf = utils.scan_parquet(
        ascwds_workplace_source, selected_columns=ASCWDS_WORKPLACE_COLUMNS_TO_IMPORT
    )
    ascwds_workplace_lf = rUtils.filter_to_first_import_of_most_recent_month(
        ascwds_workplace_lf
    )

    current_ratings_lf = rUtils.prepare_current_ratings(cqc_ratings_lf)
    historic_ratings_lf = rUtils.prepare_historic_ratings(cqc_ratings_lf)
    assessment_ratings_lf = rUtils.prepare_assessment_ratings(cqc_ratings_lf)

    ratings_lf = pl.concat(
        [current_ratings_lf, historic_ratings_lf, assessment_ratings_lf],
        how="diagonal",
    )

    ratings_lf = rUtils.recode_unknown_codes_to_null(ratings_lf)
    ratings_lf = rUtils.remove_blank_and_duplicate_rows(ratings_lf)
    ratings_lf = rUtils.add_latest_rating_flag_column(ratings_lf)
    ratings_lf = rUtils.add_numerical_ratings(ratings_lf)

    standard_ratings_lf = create_standard_ratings_dataset(ratings_lf)
    standard_ratings_lf = add_location_id_hash(standard_ratings_lf)

    benchmark_ratings_lf = select_ratings_for_benchmarks(ratings_lf)
    benchmark_ratings_lf = add_good_and_outstanding_flag_column(benchmark_ratings_lf)
    benchmark_ratings_lf = join_establishment_ids(
        benchmark_ratings_lf, ascwds_workplace_lf
    )
    benchmark_ratings_lf = create_benchmark_ratings_dataset(benchmark_ratings_lf)

    utils.sink_to_parquet(standard_ratings_lf, cqc_ratings_destination)
    utils.sink_to_parquet(benchmark_ratings_lf, benchmark_ratings_destination)


def create_standard_ratings_dataset(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Selects the standard ratings columns, then removes duplicate rows.

    Selecting first keeps `unique` to the standard columns only.

    Args:
        ratings_lf (pl.LazyFrame): CQC ratings and assessments.

    Returns:
        pl.LazyFrame: The standard columns, in output order, with duplicate rows
            removed.
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
    Adds a 20 character hash of the location ID, used to link with anonymised files.

    The hash is the first 20 characters of the SHA-256 hex digest. Polars has no
    SHA-256 expression, so `hashlib` hashes each row's location ID in a single pass.
    A join back from the distinct IDs would read the input twice.

    Args:
        ratings_lf (pl.LazyFrame): CQC ratings containing the column location_id.

    Returns:
        pl.LazyFrame: The input with an additional column containing the hashed
            location ID. A null location ID gives a null hash.
    """
    return ratings_lf.with_columns(
        pl.col(CQCL.location_id)
        .cast(pl.String)
        .map_elements(
            lambda location_id: hashlib.sha256(location_id.encode("utf-8")).hexdigest(),
            return_dtype=pl.String,
            skip_nulls=True,
        )
        .str.slice(0, 20)
        .alias(CQCRatings.location_id_hash)
    )


def select_ratings_for_benchmarks(ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Keeps current ratings of registered locations.

    Args:
        ratings_lf (pl.LazyFrame): CQC ratings containing the columns
            registration_status and current_or_historic.

    Returns:
        pl.LazyFrame: The registered locations' current ratings.
    """
    return ratings_lf.filter(
        (pl.col(CQCL.registration_status) == RegistrationStatus.registered)
        & (pl.col(CQCRatings.current_or_historic) == CQCCurrentOrHistoricValues.current)
    )


def add_good_and_outstanding_flag_column(
    benchmark_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Flags locations whose lowest overall rating value is good (3) or better.

    The flag is set on every row of a location. A location with no overall rating
    value is flagged 0.

    Args:
        benchmark_ratings_lf (pl.LazyFrame): Current ratings containing the columns
            location_id and overall_rating_value.

    Returns:
        pl.LazyFrame: The input with an additional Int32 column flagging locations
            with good or outstanding current ratings.
    """
    good_rating_value = 3

    return benchmark_ratings_lf.with_columns(
        pl.when(
            pl.col(CQCRatings.overall_rating_value).min().over(CQCL.location_id)
            >= good_rating_value
        )
        .then(1)
        .otherwise(0)
        .alias(CQCRatings.good_or_outstanding_flag)
    )


def join_establishment_ids(
    benchmark_ratings_lf: pl.LazyFrame, ascwds_workplace_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Adds the ASC-WDS establishment ID of each location.

    A location in ASC-WDS more than once keeps its lowest non-null establishment ID,
    so each rating row matches at most one establishment ID. Locations not in ASC-WDS
    get a null establishment ID.

    Args:
        benchmark_ratings_lf (pl.LazyFrame): Current ratings containing the column
            location_id.
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data containing the
            columns location_id and establishment_id.

    Returns:
        pl.LazyFrame: The input with an additional establishment_id column.
    """
    establishment_ids_lf = ascwds_workplace_lf.group_by(
        pl.col(AWP.location_id).alias(CQCL.location_id)
    ).agg(pl.col(AWP.establishment_id).min())

    return benchmark_ratings_lf.join(
        establishment_ids_lf, on=CQCL.location_id, how="left", validate="m:1"
    )


def create_benchmark_ratings_dataset(
    benchmark_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Selects and renames the benchmark columns, keeping rated locations in ASC-WDS.

    Args:
        benchmark_ratings_lf (pl.LazyFrame): Current ratings containing the columns
            location_id, establishment_id, name, dataset, good_or_outstanding_flag,
            overall_rating and rating_date.

    Returns:
        pl.LazyFrame: The benchmark columns, in output order, with rows removed where
            the establishment ID or overall rating is null.
    """
    return benchmark_ratings_lf.select(
        pl.col(CQCL.location_id).alias(CQCRatings.benchmarks_location_id),
        pl.col(AWP.establishment_id).alias(CQCRatings.benchmarks_establishment_id),
        CQCL.name,
        CQCL.dataset,
        CQCRatings.good_or_outstanding_flag,
        pl.col(CQCRatings.overall_rating).alias(CQCRatings.benchmarks_overall_rating),
        pl.col(CQCRatings.date).alias(CQCRatings.inspection_date),
    ).drop_nulls(
        [
            CQCRatings.benchmarks_establishment_id,
            CQCRatings.benchmarks_overall_rating,
        ]
    )


if __name__ == "__main__":
    print("Fargate job 'flatten_cqc_ratings' starting...")
    print(f"Job parameters: {sys.argv}")

    args = utils.get_args(
        (
            "--cqc_full_snapshot_source",
            "Source s3 directory for the latest full snapshot of CQC registered and deregistered locations dataset",
        ),
        (
            "--cqc_locations_api_delta_source",
            "Source s3 directory for raw CQC locations delta dataset",
        ),
        (
            "--ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace dataset",
        ),
        (
            "--cqc_ratings_destination",
            "Destination s3 directory for cleaned parquet CQC ratings dataset",
        ),
        (
            "--benchmark_ratings_destination",
            "Destination s3 directory for cleaned parquet benchmark ratings dataset",
        ),
    )
    main(
        args.cqc_full_snapshot_source,
        args.cqc_locations_api_delta_source,
        args.ascwds_workplace_source,
        args.cqc_ratings_destination,
        args.benchmark_ratings_destination,
    )

    print("Fargate job 'flatten_cqc_ratings' complete")
