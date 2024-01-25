import sys

from utils import utils


def main(source: str, destination: str):
    bucket, key = utils.split_s3_uri(source)
    delimiter = get_delimiter_for_csv(bucket, key)
    ascwds_worker_df = utils.read_csv(source, delimiter)

    new_destination = utils.construct_destination_path(destination, key)
    print(f"Exporting as parquet to {new_destination}")
    utils.write_to_parquet(ascwds_worker_df, new_destination)


def get_delimiter_for_csv(bucket: str, key: str):
    file_sample = utils.read_partial_csv_content(bucket, key)
    delimiter = utils.identify_csv_delimiter(file_sample)
    return delimiter


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        ("--ascwds_worker_source", "Source s3 directory for csv ascwds worker dataset"),
        (
            "--ascwds_worker_destination",
            "Destination s3 directory for cleaned parquet ascwds worker dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
