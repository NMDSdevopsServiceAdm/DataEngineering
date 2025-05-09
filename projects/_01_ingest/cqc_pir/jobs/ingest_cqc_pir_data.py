import sys
import argparse

from schemas.cqc_pir_schema import PIR_SCHEMA
from utils import utils


def main(source, destination):
    if utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket, key = utils.split_s3_uri(source)
        new_destination = utils.construct_destination_path(destination, key)
        ingest_pir_dataset(source, new_destination, PIR_SCHEMA)
        return

    print("Multiple files provided to job. Handling each file...")
    bucket, prefix = utils.split_s3_uri(source)
    objects_list = utils.get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = utils.construct_s3_uri(bucket, key)
        new_destination = utils.construct_destination_path(destination, key)
        ingest_pir_dataset(new_source, new_destination, PIR_SCHEMA)


def ingest_pir_dataset(source, destination, schema):
    print(f"Reading CSV from {source} with schema: {schema}")
    df = utils.read_csv_with_defined_schema(source, schema)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination, mode="overwrite")


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        help="A CSV file or directory of files used as job input",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'csv_to_parquet_pir' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'csv_to_parquet_pir' done")
