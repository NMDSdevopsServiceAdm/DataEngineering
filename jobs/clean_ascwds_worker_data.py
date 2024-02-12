import sys

from utils import utils
from utils.column_names.raw_data_files.ascwds_worker_columns import PartitionKeys


def main(
    worker_source: str, cleaned_workplace_source: str, cleaned_worker_destination: str
):
    ascwds_worker_df = utils.read_from_parquet(worker_source)
    ascwds_workplace_cleaned_df = utils.read_from_parquet(cleaned_workplace_source)

    print(f"Exporting as parquet to {cleaned_worker_destination}")
    utils.write_to_parquet(
        ascwds_worker_df,
        cleaned_worker_destination,
        True,
        [
            PartitionKeys.year,
            PartitionKeys.month,
            PartitionKeys.day,
            PartitionKeys.import_date,
        ],
    )


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_worker_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        worker_source,
        cleaned_workplace_source,
        cleaned_worker_destination,
    ) = utils.collect_arguments(
        (
            "--ascwds_worker_source",
            "Source s3 directory for parquet ascwds worker dataset",
        ),
        (
            "--ascwds_workplace_cleaned_source",
            "Source s3 directory for parquet ascwds workplace cleaned dataset",
        ),
        (
            "--ascwds_worker_destination",
            "Destination s3 directory for cleaned parquet ascwds worker dataset",
        ),
    )
    main(worker_source, cleaned_workplace_source, cleaned_worker_destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
