from ctypes import util
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_timestamp
from utils import utils
import sys
import pyspark
import argparse


def main(source, destination):
    if utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket_source, key = utils.split_s3_uri(source)
        sample = utils.read_partial_csv_content(bucket_source, key)
        delimiter = utils.identify_csv_delimiter(sample)
        ingest_dataset(source, destination, delimiter)
    else:
        print("Multiple files provided to job. Handling each file...")
        bucket_source, prefix = utils.split_s3_uri(source)
        objects_list = utils.get_s3_objects_list(bucket_source, prefix)
        bucket_destination = utils.split_s3_uri(destination)[0]

        print("Objects list:")
        print(objects_list)

        for key in objects_list:
            if utils.is_csv(key):
                new_source = utils.construct_s3_uri(bucket_source, key)
                dir_path = utils.get_file_directory(key)
                new_destination = utils.construct_s3_uri(
                    bucket_destination, dir_path)
                sample = utils.read_partial_csv_content(bucket_source, key)
                delimiter = utils.identify_csv_delimiter(sample)

                ingest_dataset(new_source, new_destination, delimiter)


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
