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
from utils.column_values.categorical_column_values import LocationType

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

    Commented-out calls are placeholders for steps not yet implemented. Until then,
    both destinations receive the flattened current and historic ratings.

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

    # ascwds_workplace_lf is only used by the benchmark placeholders below.
    ascwds_workplace_lf = utils.scan_parquet(
        ascwds_workplace_source, selected_columns=ASCWDS_WORKPLACE_COLUMNS_TO_IMPORT
    )
    ascwds_workplace_lf = rUtils.filter_to_first_import_of_most_recent_month(
        ascwds_workplace_lf
    )

    current_ratings_lf = rUtils.prepare_current_ratings(cqc_ratings_lf)
    historic_ratings_lf = rUtils.prepare_historic_ratings(cqc_ratings_lf)
    assessment_ratings_lf = rUtils.prepare_assessment_ratings(cqc_ratings_lf)

    rUtils.raise_error_when_assessment_lf_contains_overall_data(assessment_ratings_lf)

    ratings_lf = pl.concat([current_ratings_lf, historic_ratings_lf], how="diagonal")
    # ratings_lf = merge_cqc_ratings(assessment_ratings_lf, ratings_lf)

    # ratings_lf = recode_unknown_codes_to_null(ratings_lf)
    # ratings_lf = remove_blank_and_duplicate_rows(ratings_lf)
    # ratings_lf = add_rating_sequence_column(ratings_lf)
    # ratings_lf = add_rating_sequence_column(ratings_lf, reversed=True)
    # ratings_lf = add_latest_rating_flag_column(ratings_lf)
    # ratings_lf = add_numerical_ratings(ratings_lf)

    standard_ratings_lf = ratings_lf
    # standard_ratings_lf = create_standard_ratings_dataset(ratings_lf)
    # standard_ratings_lf = add_location_id_hash(standard_ratings_lf)

    benchmark_ratings_lf = ratings_lf
    # benchmark_ratings_lf = select_ratings_for_benchmarks(ratings_lf)
    # benchmark_ratings_lf = add_good_and_outstanding_flag_column(benchmark_ratings_lf)
    # benchmark_ratings_lf = join_establishment_ids(benchmark_ratings_lf, ascwds_workplace_lf)
    # benchmark_ratings_lf = create_benchmark_ratings_dataset(benchmark_ratings_lf)

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
    SHA-256 expression, so `hashlib` hashes the distinct location IDs only and the
    result is joined back.

    Args:
        ratings_lf (pl.LazyFrame): CQC ratings containing the column location_id.

    Returns:
        pl.LazyFrame: The input with an additional column containing the hashed
            location ID. A null location ID gives a null hash.
    """
    location_id_hash_lf = (
        ratings_lf.select(CQCL.location_id)
        .unique()
        .with_columns(
            pl.col(CQCL.location_id)
            .cast(pl.String)
            .map_elements(
                lambda location_id: hashlib.sha256(
                    location_id.encode("utf-8")
                ).hexdigest()[:20],
                return_dtype=pl.String,
            )
            .alias(CQCRatings.location_id_hash)
        )
    )
    return ratings_lf.join(
        location_id_hash_lf, on=CQCL.location_id, how="left", validate="m:1"
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
