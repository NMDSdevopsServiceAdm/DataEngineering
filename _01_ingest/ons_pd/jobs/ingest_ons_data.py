import sys

from _01_ingest.utils.utils import ingest_utils
from utils import utils


def main(source, destination):
    if ingest_utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket, key = ingest_utils.split_s3_uri(source)
        print(destination)
        new_destination = ingest_utils.construct_destination_path(destination, key)
        print(new_destination)
        handle_job(source, bucket, key, new_destination)
        return

    print("Multiple files provided to job. Handling each file...")
    bucket, prefix = ingest_utils.split_s3_uri(source)
    objects_list = ingest_utils.get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = ingest_utils.construct_s3_uri(bucket, key)
        new_destination = ingest_utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(source: str, source_bucket: str, source_key: str, destination: str):
    file_sample = ingest_utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = ingest_utils.identify_csv_delimiter(file_sample)
    ingest_dataset(source, destination, delimiter)


def ingest_dataset(source: str, destination: str, delimiter: str):
    print(
        f"Reading CSV from {source} and writing to {destination} with delimiter: {delimiter}"
    )
    df = ingest_utils.read_csv(source, delimiter)
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
