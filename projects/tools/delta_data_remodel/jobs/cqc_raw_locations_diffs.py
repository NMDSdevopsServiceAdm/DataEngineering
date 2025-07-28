import polars as pl

from raw_locations_schema import raw_locations_schema
from projects.tools.delta_data_remodel.jobs.utils import list_bucket_objects, get_diffs
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCP,
)


def main():
    dataset_cols = raw_locations_schema.names()
    dataset_cols.remove(CQCP.location_id)  # primary key
    dataset_cols.remove(
        CQCP.main_phone_number
    )  # don't check for changes in phone number as sometimes in exponential format

    read_bucket = "sfc-main-datasets"
    write_bucket = "sfc-delta-locations-dataset-datasets"
    read_folder = (
        Rf"domain=CQC/dataset=locations_api/version=2.1.1/year=2013/month=03/day=01/"
    )
    write_folder = Rf"domain=CQC/dataset=locations_api/version=3.0.0/year=2013/month=03/day=01/import_date=20130301/file.parquet"

    base_df = pl.scan_parquet(
        f"s3://{read_bucket}/{read_folder}",
        schema=raw_locations_schema,
        cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
    ).collect()

    base_df.drop(["import_date"]).write_parquet(
        f"s3://{write_bucket}/{write_folder}/",
        compression="snappy",
        partition_chunk_size_bytes=220000,
    )

    print(f"Original entries: {base_df.shape}")

    for year in range(2013, 2026):
        for month in range(1, 13):
            if year == 2013 and month <= 3:
                continue
            elif year == 2025 and month > 7:
                break

            day_folders = list_bucket_objects(
                "sfc-main-datasets",
                f"domain=CQC/dataset=locations_api/version=2.1.1/year={year}/month={month:02}",
            )

            for read_folder in sorted(day_folders):
                day = int(read_folder[-2:])
                print(f"Starting {day}-{month}-{year}")
                write_folder = Rf"domain=CQC/dataset=locations_api/version=3.0.0/year={year}/month={month:02}/day={day:02}/import_date={year}{month:02}{day:02}/"

                snapshot_df = pl.scan_parquet(
                    f"s3://{read_bucket}/{read_folder}/",
                    schema=raw_locations_schema,
                    cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
                ).collect()  # as of 01/03/2022 there is a removed field in regulatedActivities of 'col3', so using scan to take advantage of cast options

                changed_entries = get_diffs(
                    base_df,
                    snapshot_df,
                    snapshot_date=f"{year}{month:02}{day:02}",
                    primary_key=CQCP.location_id,
                    change_cols=dataset_cols,
                )

                base_df = snapshot_df

                changed_entries.write_parquet(
                    f"s3://{write_bucket}/{write_folder}file.parquet",
                    compression="snappy",
                )


if __name__ == "__main__":
    main()
