from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    merged_ind_cqc_source: str,
    destination: str,
) -> None:
    """
    Cleans independent CQC locations data.

    Args:
        merged_ind_cqc_source (str): s3 path to the merged independent CQC location data
        destination (str): s3 path to save cleaned independent CQC location data
    """
    print("Cleaning merged_ind_cqc dataset...")

    locations_lf = utils.scan_parquet(merged_ind_cqc_source)
    print("Merged independent CQC location LazyFrame read in")

    utils.sink_to_parquet(
        locations_lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Clean Independent CQC job")

    args = utils.get_args(
        (
            "--merged_ind_cqc_source",
            "S3 URI to read merged CQC location data from",
        ),
        (
            "--destination",
            "S3 URI to save cleaned ind cqc data to",
        ),
    )

    main(
        merged_ind_cqc_source=args.merged_ind_cqc_source,
        destination=args.destination,
    )

    print("Finished Clean Independent CQC job")
