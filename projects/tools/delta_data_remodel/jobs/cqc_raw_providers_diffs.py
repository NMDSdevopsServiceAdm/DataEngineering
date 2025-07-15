from polars import read_parquet

from raw_providers_schema import raw_providers_schema
from utils import list_bucket_objects, get_diffs


def main():
    dataset_cols1 = raw_providers_schema.names()
    dataset_cols1.remove("providerId")  # primary key
    dataset_cols1.remove(
        "mainPhoneNumber"
    )  # don't check for changes in phone number as sometimes in exponential format

    bucket = "sfc-main-datasets"
    write_bucket = "sfc-test-diff-datasets"
    read_folder = (
        Rf"domain=CQC/dataset=providers_api/version=2.0.0/year=2013/month=03/day=01/"
    )
    write_folder = Rf"domain=CQC/dataset=providers_api/year=2013/month=03/day=01/import_date=20130301/file.parquet"

    base_df = read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_providers_schema,
    )

    base_df.write_parquet(
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
                f"domain=CQC/dataset=providers_api/version=2.0.0/year={year}/month={month:02}",
            )

            for read_folder in sorted(day_folders):
                day = int(read_folder[-2:])
                print(f"Starting {day}-{month}-{year}")
                write_folder = Rf"domain=CQC/dataset=providers_api/year={year}/month={month:02}/day={day:02}/import_date={year}{month:02}{day:02}/"

                snapshot_df = read_parquet(
                    f"s3://{bucket}/{read_folder}/",
                    schema=raw_providers_schema,
                )

                changed_entries = get_diffs(
                    base_df,
                    snapshot_df,
                    snapshot_date=f"{year}{month:02}{day:02}",
                    primary_key="providerId",
                    change_cols=dataset_cols,
                )

                base_df = snapshot_df

                changed_entries.write_parquet(
                    f"s3://{write_bucket}/{write_folder}file.parquet",
                    compression="snappy",
                )


if __name__ == "__main__":
    main()
