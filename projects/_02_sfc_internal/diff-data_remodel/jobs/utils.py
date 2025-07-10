from re import match

import boto3
from polars import read_parquet, concat, col, lit, Int64

from raw_providers_delta_schema import raw_providers_schema


def download_from_s3(src_bucket, src_filename, dest_filename):
    s3 = boto3.client("s3")
    s3.download_file(
        src_bucket,
        src_filename,
        dest_filename,
    )


def list_bucket_objects(bucket, prefix):
    s3 = boto3.client("s3")
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in objects["Contents"]]


def build_full_table_from_delta(bucket, read_folder):
    delta_df = read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_providers_schema,
    )

    previous_ss = None
    timepoints = []

    for last_updated, delta_data in delta_df.group_by(
        "last_updated", maintain_order=True
    ):
        print(last_updated[0])
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        date = match(date_pattern, last_updated[0])

        if last_updated[0] == "20130301":
            previous_ss = delta_data
        else:
            unchanged = previous_ss.remove(
                col("providerId").is_in(delta_data["providerId"])
            )
            changed = delta_data.filter(
                col("providerId").is_in(previous_ss["providerId"])
            ).remove(col("deregistrationDate").ne(""))
            deleted = delta_data.filter(col("deregistrationDate").ne(""))
            new = delta_data.remove(col("providerId").is_in(previous_ss["providerId"]))
            print(f"Changed records: {changed.shape[0]}")
            print(f"Unchanged records: {unchanged.shape[0]}")
            print(f"Total records: {delta_data.shape[0]}")

            previous_ss = concat([unchanged, changed, new])
            previous_ss = previous_ss.with_columns(
                # lit(date.group("year")).alias("year").cast(Int64),
                lit(date.group("month")).alias("month").cast(Int64),
                lit(date.group("day")).alias("day").cast(Int64),
                lit(last_updated[0]).alias("import_date").cast(Int64),
            )

            print(f"Full SS records: {previous_ss.shape[0]}")

        timepoints.append(previous_ss)

    full_df = concat(timepoints)
    return full_df
