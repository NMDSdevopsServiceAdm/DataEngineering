import polars as pl

from raw_providers_schema import raw_providers_schema
from projects.tools.delta_data_remodel.jobs.utils import list_bucket_objects, get_diffs
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


def main():
    dataset_cols = raw_providers_schema.names()
    dataset_cols.remove(CQCP.provider_id)  # primary key
    dataset_cols.remove(
        CQCP.main_phone_number
    )  # don't check for changes in phone number as sometimes in exponential format

    bucket = "sfc-main-datasets"
    write_bucket = "sfc-main-datasets"
    read_folder = (
        Rf"domain=CQC/dataset=providers_api/version=2.0.0/year=2013/month=03/day=01/"
    )
    write_folder = Rf"domain=CQC_delta/dataset=providers_api/version=3.0.0/year=2013/month=03/day=01/import_date=20130301/file.parquet"

    base_df = pl.read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_providers_schema,
    )

    base_df.drop([Keys.import_date]).write_parquet(
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
                write_folder = Rf"domain=CQC_delta/dataset=providers_api/version=3.0.0/year={year}/month={month:02}/day={day:02}/import_date={year}{month:02}{day:02}/"

                snapshot_df = pl.read_parquet(
                    f"s3://{bucket}/{read_folder}/",
                    schema=raw_providers_schema,
                )

                changed_entries = get_diffs(
                    base_df,
                    snapshot_df,
                    snapshot_date=f"{year}{month:02}{day:02}",
                    primary_key=CQCP.provider_id,
                    change_cols=dataset_cols,
                )

                base_df = snapshot_df

                changed_entries.drop([Keys.import_date]).write_parquet(
                    f"s3://{write_bucket}/{write_folder}file.parquet",
                    compression="snappy",
                )


if __name__ == "__main__":
    main()
