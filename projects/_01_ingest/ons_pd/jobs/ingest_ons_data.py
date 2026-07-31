import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from utils import utils
from utils.s3_file_utils import (
    construct_destination_path,
    construct_s3_uri,
    get_s3_objects_list,
    identify_csv_delimiter,
    is_csv,
    read_partial_csv_content,
    split_s3_uri,
)


def main(source, destination):
    if is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket, key = split_s3_uri(source)
        print(destination)
        new_destination = construct_destination_path(destination, key)
        print(new_destination)
        handle_job(source, bucket, key, new_destination)
        return

    print("Multiple files provided to job. Handling each file...")
    bucket, prefix = split_s3_uri(source)
    objects_list = get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = construct_s3_uri(bucket, key)
        new_destination = construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(source: str, source_bucket: str, source_key: str, destination: str):
    file_sample = read_partial_csv_content(source_bucket, source_key)
    delimiter = identify_csv_delimiter(file_sample)
    ingest_dataset(source, destination, delimiter)


def ingest_dataset(source: str, destination: str, delimiter: str):
    print(
        f"Reading CSV from {source} and writing to {destination} with delimiter: {delimiter}"
    )
    df = utils.read_csv(source, delimiter)
    df = utils.format_date_fields(df, raw_date_format="dd/MM/yyyy")

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination, mode="overwrite")


if __name__ == "__main__":
    print("Spark job 'inges_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    ons_source, ons_destination = utils.collect_arguments(
        (
            "--source",
            "A CSV file or directory of csv files in s3 with ONS data to import",
        ),
        (
            "--destination",
            "Destination s3 directory for ONS postcode directory",
        ),
    )
    main(ons_source, ons_destination)
    print("Spark job 'ingest_ons_data' complete")
