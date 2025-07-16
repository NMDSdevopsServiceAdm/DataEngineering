"""
PLEASE NOTE: THIS FILE IS NOT PRODUCTION READY AND MAY NOT RUN
"""

from polars import read_parquet, concat, col
import polars as pl
from polars.testing import assert_frame_equal

from projects.tools.delta_data_remodel.jobs.utils import get_diffs


def main():
    dataset_cols = [
        "postalCode",
        "providerId",
        "careHome",
        "dormancy",
        "gacServiceTypes",
        "name",
        "numberOfBeds",
        "registrationStatus",
        "registrationDate",
        "deregistrationDate",
        "regulatedActivities",
        "specialisms",
        "type",
        "relationships",
        "imputed_registration_date",
        "imputed_relationships",
        "imputed_gacServiceTypes",
        "imputed_regulatedActivities",
        "imputed_specialisms",
        "services_offered",
        "specialisms_offered",
        "primary_service_type",
        "registered_manager_names",
        "related_location",
        "provider_name",
        "cqc_sector",
        "contemporary_CSSR",
        "contemporary_Region",
        "contemporary_Sub_ICB",
        "contemporary_ICB",
        "contemporary_ICB_Region",
        "contemporary_CCG",
        "contemporary_lat",
        "contemporary_long",
        "contemporary_imd",
        "contemporary_lsoa11",
        "contemporary_msoa11",
        "contemporary_ru11ind",
        "contemporary_lsoa21",
        "contemporary_msoa21",
        "contemporary_pcon",
        "current_CSSR",
        "current_Region",
        "current_Sub_ICB",
        "current_ICB",
        "current_ICB_Region",
        "current_CCG",
        "current_lat",
        "current_long",
        "current_imd",
        "current_lsoa11",
        "current_msoa11",
        "current_ru11ind",
        "current_lsoa21",
        "current_msoa21",
        "current_pcon",
    ]

    bucket = "sfc-main-datasets"
    write_bucket = "sfc-test-diff-datasets"
    read_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month=03/day=01/import_date=20130301/"
    write_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month=03/"

    base_df = read_parquet(f"s3://{bucket}/{read_folder}")

    base_df = base_df.with_columns(
        pl.lit("20130301").alias("last_updated"),
        pl.lit(False).alias("db_delete"),
    )
    base_df.write_parquet(
        f"s3://{write_bucket}/{write_folder}/",
        compression="snappy",
        use_pyarrow=True,
        pyarrow_options={"partition_cols": ["last_updated"]},
        partition_chunk_size_bytes=220000,
    )

    print(f"Original entries: {base_df.shape}")

    for year in range(2013, 2015):
        for month in range(1, 13):
            if year == 2013:
                if month < 4:
                    continue
            print(f"Starting month {month}-{year}")

            write_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year={year}/month={month:02}/"
            read_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year={year}/month={month:02}/day=01/import_date={year}{month:02}01/"

            print(f"Reading from {read_folder}")
            snapshot_df = read_parquet(f"s3://{bucket}/{read_folder}")

            print(
                f"Unique Last Updated before join: {base_df['last_updated'].value_counts()}"
            )

            base_df, changed_entries = get_diffs(
                base_df,
                snapshot_df,
                snapshot_date=f"{year}{month:02}01",
                primary_key="locationId",
                change_cols=dataset_cols,
            )

            print(
                f"Unique Last Updated after join: {base_df['last_updated'].value_counts()}"
            )

            changed_entries.write_parquet(
                f"s3://{write_bucket}/{write_folder}/",
                compression="snappy",
                use_pyarrow=True,
                pyarrow_options={"partition_cols": ["last_updated"]},
                partition_chunk_size_bytes=220000,
            )


if __name__ == "__main__":
    main()
