import boto3
import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import RegistrationStatus

BUCKET_NAME = "sfc-1036-fix-locations-full-datasets"
BACKUP_PREFIX = (
    "domain=CQC_delta_backupregistration/dataset=full_locations_api/version=3.0.0"
)
LIVE_PREFIX = "domain=CQC_delta/dataset=full_locations_api/version=3.0.0"
s3 = boto3.client("s3")


def _get_prefixes(bucket, prefix):
    s3_objs = s3.list_objects(Bucket=BUCKET_NAME, Delimiter="/", Prefix=prefix)
    prefixes = s3_objs.get("CommonPrefixes", list())

    if prefixes == []:
        return [prefix]

    all_folders = []
    for prefix in prefixes:
        all_folders.extend(_get_prefixes(bucket, prefix["Prefix"]))

    return all_folders


for folder in _get_prefixes(BUCKET_NAME, BACKUP_PREFIX)[:5]:
    print(f"Reading {folder}")
    read_df = pl.read_parquet(f"s3://{BUCKET_NAME}/{folder}")
    print("Before")
    print(read_df[CQCLClean.registration_status].value_counts())
    write_df = read_df.with_columns(
        pl.when(pl.col(CQCLClean.deregistration_date).is_not_null())
        .then(pl.lit(RegistrationStatus.deregistered))
        .otherwise(CQCLClean.registration_status)
        .alias(CQCLClean.registration_status)
    )
    print("After")
    print(write_df[CQCLClean.registration_status].value_counts())
    write_folder = folder.replace(
        "domain=CQC_delta_backupregistration", "domain=CQC_delta"
    )
    print(f"Writing {write_folder}")
    # write_df.write_parquet(f"s3://{BUCKET_NAME}/{write_folder}")
