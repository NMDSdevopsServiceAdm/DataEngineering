from ctypes import util
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_timestamp
from utils import utils
import sys
import pyspark
import argparse


def main(source, destination):
    bucket, key = utils.split_s3_uri(source)

    if utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(source, bucket, key, new_destination)
        return
    
    print("Multiple files provided to job. Handling each file...")
    objects_list = utils.get_s3_objects_list(bucket, key) # here key is actually prefix

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = utils.construct_s3_uri(bucket, key)
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(source, source_bucket, source_key, destination):
    file_sample = utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = utils.identify_csv_delimiter(sample)
    ingest_dataset(source, destination, delimiter)


def ingest_dataset(source, destination, delimiter):
    print(
        f"Reading CSV from {source} and writing to {destination} with delimiter: {delimiter}")
    df = utils.read_csv(source, delimiter)
    df = filter_test_accounts(df)
    df = utils.format_date_fields(df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination)


def filter_test_accounts(df):
    test_accounts = ["305", "307", "308", "309",
                     "310", "2452", "28470", "26792", "31657"]

    if "orgid" in df.columns:
        df = df.filter(~df.orgid.isin(test_accounts))

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file or directory of files used as job input", required=True)
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
