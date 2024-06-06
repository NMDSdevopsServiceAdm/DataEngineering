import sys
import argparse
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils import utils
from utils.raw_data_adjustments import remove_duplicate_worker_in_raw_worker_data


def main(source: str, destination: str):
    if utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket, key = utils.split_s3_uri(source)
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(source, bucket, key, new_destination)
        return

    print("Multiple files provided to job. Handling each file...")
    bucket, prefix = utils.split_s3_uri(source)
    objects_list = utils.get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = utils.construct_s3_uri(bucket, key)
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(source: str, source_bucket: str, source_key: str, destination: str):
    file_sample = utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = utils.identify_csv_delimiter(file_sample)
    ingest_dataset(source, destination, delimiter)


def ingest_dataset(source: str, destination: str, delimiter: str):
    print(
        f"Reading CSV from {source} and writing to {destination} with delimiter: {delimiter}"
    )
    df = utils.read_csv(source, delimiter)
    df = filter_test_accounts(df)
    df = remove_white_space_from_nmdsid(df)
    df = remove_duplicate_worker_in_raw_worker_data(df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination)


def filter_test_accounts(df: DataFrame) -> DataFrame:
    test_accounts = [
        "305",
        "307",
        "308",
        "309",
        "310",
        "2452",
        "28470",
        "26792",
        "31657",
        "31138",
    ]

    if "orgid" in df.columns:
        df = df.filter(~df.orgid.isin(test_accounts))

    return df


def remove_white_space_from_nmdsid(df: DataFrame) -> DataFrame:
    return df.withColumn("nmdsid", F.trim(F.col("nmdsid")))


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
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
