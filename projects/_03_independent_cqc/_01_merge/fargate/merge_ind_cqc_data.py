from polars_utils import utils
from projects._03_independent_cqc._01_merge.fargate.utils.merge_utils import (
    join_data_into_cqc_lf,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    cleaned_ct_non_res_source: str,
    cleaned_ct_care_home_source: str,
    destination: str,
) -> None:
    cleaned_cqc_location_lf = utils.scan_parquet(
        cleaned_cqc_location_source,
    )
    print("Cleaned CQC location LazyFrame read in")

    cleaned_cqc_pir_lf = utils.scan_parquet(
        cleaned_cqc_pir_source,
    )
    print("Cleaned CQC PIR LazyFrame read in")

    cleaned_ascwds_workplacen_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source,
    )
    print("Cleaned ASCWDS workplace LazyFrame read in")

    cleaned_ct_non_res_lf = utils.scan_parquet(
        cleaned_ct_non_res_source,
    )
    print("Cleaned capacity tracker non-residential LazyFrame read in")

    cleaned_ct_care_home_lf = utils.scan_parquet(
        cleaned_ct_care_home_source,
    )
    print("Cleaned capacity tracker care home LazyFrame read in")

    utils.sink_to_parquet(
        cleaned_cqc_location_lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Merge Independent CQC job")

    args = utils.get_args(
        (
            "--cleaned_cqc_location_source",
            "S3 URI to read cleaned CQC location data from",
        ),
        (
            "--cleaned_cqc_pir_sourc",
            "S3 URI to read cleaned CQC PIR data from",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "S3 URI to read cleaned ASCWDS workplace data from",
        ),
        (
            "--cleaned_ct_non_res_source",
            "S3 URI to read cleaned capacity tracker non-residential data from",
        ),
        (
            "--cleaned_ct_care_home_source",
            "S3 URI to read cleaned capacity tracker care home data from",
        ),
        (
            "--destination",
            "S3 URI to save merged data to",
        ),
    )

    main(
        cleaned_cqc_location_source=args.cleaned_cqc_location_source,
        cleaned_cqc_pir_source=args.cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        cleaned_ct_non_res_source=args.cleaned_ct_non_res_source,
        cleaned_ct_care_home_source=args.cleaned_ct_care_home_source,
        destination=args.destination,
    )

    print("Finished Merge Independent CQC job")
