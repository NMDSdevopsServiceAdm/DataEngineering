from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    source: str,
    destination: str,
) -> None:
    lf = utils.scan_parquet(
        source,
    )
    print("LazyFrame read in")

    utils.sink_to_parquet(
        lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Merge Independent CQC job")

    args = utils.get_args(
        (
            "--source",
            "S3 URI to read some data from",
        ),
        (
            "--destination",
            "S3 URI to save some data to",
        ),
    )

    main(
        source=args.source,
        destination=args.destination,
    )

    print("Finished Merge Independent CQC job")
