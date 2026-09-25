import polars as pl

from polars_utils import utils
from projects._02_sfc_internal._01_cqc_ratings.fargate.utils import (
    utils as ratings_utils,
)
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
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

delta_columns = [
    CQCL.location_id,
    Keys.import_date,
    CQCL.current_ratings,
    CQCL.historic_ratings,
    CQCL.assessment,
]

snapshot_columns = [
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.type,
]

ascwds_workplace_columns = [
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
    """
    Builds the standard and benchmark CQC ratings datasets from raw CQC and ASC-WDS data.

    Args:
        cqc_full_snapshot_source (str): S3 URI to read the latest full snapshot of CQC
            registered and deregistered locations from.
        cqc_locations_api_delta_source (str): S3 URI to read the raw CQC locations API
            delta dataset from.
        ascwds_workplace_source (str): S3 URI to read the parquet ASC-WDS workplace
            dataset from.
        cqc_ratings_destination (str): S3 URI to write the cleaned CQC ratings dataset to.
        benchmark_ratings_destination (str): S3 URI to write the cleaned benchmark
            ratings dataset to.
    """
    cqc_latest_full_lf = utils.scan_parquet(
        cqc_full_snapshot_source, selected_columns=snapshot_columns
    ).filter(pl.col(CQCL.type) == LocationType.social_care_identifier)

    cqc_delta_raw_lf = utils.scan_parquet(
        cqc_locations_api_delta_source,
        schema=POLARS_LOCATION_SCHEMA,
        selected_columns=delta_columns,
    )
    cqc_latest_delta_raw_lf = ratings_utils.keep_latest_per_key(
        cqc_delta_raw_lf, CQCL.location_id, Keys.import_date
    )

    cqc_ratings_lf = cqc_latest_full_lf.join(
        cqc_latest_delta_raw_lf, on=CQCL.location_id, how="left"
    )

    ascwds_workplace_lf = utils.scan_parquet(
        ascwds_workplace_source, selected_columns=ascwds_workplace_columns
    )
    ascwds_workplace_lf = ratings_utils.filter_to_first_import_of_most_recent_month(
        ascwds_workplace_lf
    )

    current_ratings_lf = ratings_utils.prepare_current_ratings(cqc_ratings_lf)
    historic_ratings_lf = ratings_utils.prepare_historic_ratings(cqc_ratings_lf)
    assessment_ratings_lf = ratings_utils.prepare_assessment_ratings(cqc_ratings_lf)

    ratings_utils.raise_error_when_assessment_df_contains_overall_data(
        assessment_ratings_lf
    )

    ratings_pre_saf_lf = pl.concat(
        [current_ratings_lf, historic_ratings_lf], how="diagonal_relaxed"
    )
    ratings_lf = ratings_utils.merge_cqc_ratings(
        assessment_ratings_lf, ratings_pre_saf_lf
    )

    ratings_lf = ratings_utils.recode_unknown_codes_to_null(ratings_lf)
    ratings_lf = ratings_utils.remove_blank_and_duplicate_rows(ratings_lf)
    ratings_lf = ratings_utils.add_rating_sequence_column(ratings_lf)
    ratings_lf = ratings_utils.add_rating_sequence_column(ratings_lf, reversed=True)
    ratings_lf = ratings_utils.add_latest_rating_flag_column(ratings_lf)
    ratings_lf = ratings_utils.add_numerical_ratings(ratings_lf)

    # Single collect: this is the point where the shared ratings data (already
    # deduplicated and reduced to one row per location/rating period) diverges
    # into the two final, independent outputs below.
    ratings_lf = ratings_lf.collect().lazy()

    standard_ratings_lf = ratings_utils.create_standard_ratings_dataset(ratings_lf)
    standard_ratings_lf = ratings_utils.add_location_id_hash(standard_ratings_lf)

    benchmark_ratings_lf = ratings_utils.select_ratings_for_benchmarks(ratings_lf)
    benchmark_ratings_lf = ratings_utils.add_good_and_outstanding_flag_column(
        benchmark_ratings_lf
    )
    benchmark_ratings_lf = ratings_utils.join_establishment_ids(
        benchmark_ratings_lf, ascwds_workplace_lf
    )
    benchmark_ratings_lf = ratings_utils.create_benchmark_ratings_dataset(
        benchmark_ratings_lf
    )

    utils.sink_to_parquet(standard_ratings_lf, cqc_ratings_destination)
    utils.sink_to_parquet(benchmark_ratings_lf, benchmark_ratings_destination)


if __name__ == "__main__":
    print("Running Flatten CQC Ratings job")

    args = utils.get_args(
        (
            "--cqc_full_snapshot_source",
            "S3 URI to read the latest full snapshot of CQC registered and deregistered locations from",
        ),
        (
            "--cqc_locations_api_delta_source",
            "S3 URI to read the raw CQC locations API delta dataset from",
        ),
        (
            "--ascwds_workplace_source",
            "S3 URI to read the parquet ASC-WDS workplace dataset from",
        ),
        (
            "--cqc_ratings_destination",
            "S3 URI to write the cleaned CQC ratings dataset to",
        ),
        (
            "--benchmark_ratings_destination",
            "S3 URI to write the cleaned benchmark ratings dataset to",
        ),
    )

    main(
        cqc_full_snapshot_source=args.cqc_full_snapshot_source,
        cqc_locations_api_delta_source=args.cqc_locations_api_delta_source,
        ascwds_workplace_source=args.ascwds_workplace_source,
        cqc_ratings_destination=args.cqc_ratings_destination,
        benchmark_ratings_destination=args.benchmark_ratings_destination,
    )

    print("Finished Flatten CQC Ratings job")
