import sys

from utils import utils
from utils.column_names.raw_data_files.ascwds_worker_columns import PartitionKeys


def main(source: str, destination: str):
    ascwds_worker_df = utils.read_from_parquet(source)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(
        ascwds_worker_df,
        destination,
        True,
        [
            PartitionKeys.year,
            PartitionKeys.month,
            PartitionKeys.day,
            PartitionKeys.import_date,
        ],
    )


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--ascwds_worker_source",
            "Source s3 directory for parquet ascwds worker dataset",
        ),
        (
            "--ascwds_worker_destination",
            "Destination s3 directory for cleaned parquet ascwds worker dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
