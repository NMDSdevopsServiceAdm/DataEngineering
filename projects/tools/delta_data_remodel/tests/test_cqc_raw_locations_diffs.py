import os
import unittest
from re import match

import polars as pl
import requests
import json

from projects.tools.delta_data_remodel.jobs.utils import (
    build_full_table_from_delta,
    list_bucket_objects,
)
from lambdas.utils.snapshots import get_snapshots
from projects.tools.delta_data_remodel.jobs.raw_locations_schema import (
    raw_locations_schema,
)


@unittest.skip("should be run manually")
def test_rebuilt_dataset_equality():
    full_from_delta = (
        build_full_table_from_delta(
            bucket="sfc-delta-locations-dataset-datasets",
            read_folder="domain=CQC/dataset=locations_api/version=3.0.0/",
            organisation_type="locations",
            timepoint_limit=20131231,
        )
        .drop(["mainPhoneNumber", "year"])
        .remove(pl.col("deregistrationDate").ne(""))
    )

    original_format = (
        pl.scan_parquet(
            "s3://sfc-main-datasets/domain=CQC/dataset=locations_api/version=2.1.1/year=2013/",
            schema=raw_locations_schema,
            cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        )
        .collect()
        .drop(["mainPhoneNumber"])
        .remove(pl.col("deregistrationDate").ne(""))
    )

    pl.testing.assert_frame_equal(
        full_from_delta,
        original_format,
        check_row_order=False,
    )


@unittest.skip("should be run manually")
def test_snapshot_equality():
    for delta_snapshot in get_snapshots(
        bucket="sfc-delta-locations-dataset-datasets",
        read_folder="domain=CQC/dataset=locations_api/version=3.0.0/",
        organisation_type="locations",
    ):
        timepoint_int = delta_snapshot.item(1, "import_date")
        date_pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})"
        t = match(date_pattern, f"{timepoint_int}")

        old_snapshot = (
            pl.scan_parquet(
                f"s3://sfc-main-datasets/domain=CQC/dataset=locations_api/version=2.1.1/year={t.group('year')}/month={t.group('month')}/day={t.group('day')}/",
                schema=raw_locations_schema,
                cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
            )
            .collect()
            .filter(pl.col("deregistrationDate").is_null())
        )

        pl.testing.assert_frame_equal(
            delta_snapshot.drop(["year", "month", "day", "mainPhoneNumber"]).filter(
                pl.col("deregistrationDate").is_null()
            ),
            old_snapshot.drop(["mainPhoneNumber"]),
            check_row_order=False,
        )
        print(f"Timepoint {timepoint_int} is validated")


@unittest.skip("should be run manually")
def evaluate_dataset_changes():
    dataset_cols = raw_locations_schema.names()
    dataset_cols.remove("locationId")  # primary key
    dataset_cols.remove(
        "mainPhoneNumber"
    )  # don't check for changes in phone number as sometimes in exponential format
    df_a = pl.read_parquet(
        "s3://sfc-main-datasets/domain=CQC/dataset=providers_api/version=2.0.0/year=2024/month=04/day=08/import_date=20240408/"
    )
    df_b = pl.read_parquet(
        "s3://sfc-main-datasets/domain=CQC/dataset=providers_api/version=2.0.0/year=2024/month=04/day=15/import_date=20240415/"
    )

    unchanged_conditions = []
    for col_name in dataset_cols:
        unchanged_conditions.append(
            (pl.col(f"{col_name}").eq_missing(pl.col(f"{col_name}_base")))
        )

    joined_df = df_b.join(
        df_a, on="locationId", how="left", suffix="_base", maintain_order="right"
    )

    rows_with_changes = joined_df.remove(unchanged_conditions)
    print(f"Total Changes = {rows_with_changes.shape[0]}")
    comparison = rows_with_changes.with_columns(unchanged_conditions)
    print(rows_with_changes.head())
    print(comparison.sum().select(dataset_cols).glimpse())


@unittest.skip("should be run manually")
def test_delta_matches_changes_api():
    bucket = "sfc-delta-locations-dataset-datasets"
    prev_year = 2024
    prev_month = 12
    prev_day = 23
    for year in [2025]:
        for month in range(1, 2):
            if year == 2025 and month >= 8:
                break

            print(
                f"domain=CQC/dataset=locations_api/version=3.0.0/year={year}/month={month:02}"
            )

            day_folders = list_bucket_objects(
                bucket,
                f"domain=CQC/dataset=locations_api/version=3.0.0/year={year}/month={month:02}",
            )

            for day in [1, 8, 15, 23]:
                read_folder = Rf"domain=CQC/dataset=providers_api/year={year}/month={month:02}/day={day:02}"
                if read_folder not in day_folders:
                    continue

                delta_df = pl.read_parquet(
                    f"s3://{bucket}/{read_folder}/import_date={year}{month:02}{day:02}/"
                )
                start_timestamp = f"{prev_year}-{prev_month:02}-{prev_day:02}T00:00:00Z"
                end_timestamp = f"{year}-{month:02}-{day:02}T00:00:00Z"
                headers = {
                    "User-Agent": "MadeTech",
                    "Ocp-Apim-Subscription-Key": os.getenv("OCP_API_KEY"),
                }

                api_response = requests.get(
                    f"https://api.service.cqc.org.uk/public/v1/changes/provider?startTimestamp={start_timestamp}&endTimestamp={end_timestamp}",
                    headers=headers,
                )

                api_changes = json.loads(api_response.text)["changes"]

                extra_d_changes = delta_df.remove(
                    pl.col("providerId").is_in(api_changes)
                )
                matching_changes = delta_df.filter(
                    pl.col("providerId").is_in(api_changes)
                )
                missing_d_changes = [
                    c for c in api_changes if c not in delta_df["providerId"].to_list()
                ]
                print(
                    f"FOR {prev_year}{prev_month:02}{prev_day:02} TO {year}{month:02}{day:02}"
                )
                print(f"TOTAL DELTA CHANGES: {delta_df.shape[0]}")
                print(f"TOTAL API CHANGES: {len(api_changes)}")
                print("=========================================")
                print(f"Matching Changes = {matching_changes.shape[0]}")
                print(f"Changes in Delta not in API = {extra_d_changes.shape[0]}")
                print(f"Changes in API not in Delta = {len(missing_d_changes)}")
                print(missing_d_changes[:5])
                print("\n")

                prev_year = year
                prev_month = month
                prev_day = day


if __name__ == "__main__":
    test_snapshot_equality()
