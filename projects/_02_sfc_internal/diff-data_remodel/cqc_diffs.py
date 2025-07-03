from polars import read_parquet, concat, col
import polars as pl
from polars.testing import assert_frame_equal

from utils import download_from_s3, list_bucket_objects


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
    print(base_df.schema)

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

    for month in range(4, 6):
        print(f"Starting month {month}")

        write_folder = (
            Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month={month:02}/"
        )
        read_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month={month:02}/day=01/import_date=2013{month:02}01/"

        print(f"Reading from {read_folder}")
        snapshot_df = read_parquet(f"s3://{bucket}/{read_folder}")

        removed_entries = base_df.join(snapshot_df, how="anti", on="locationId")
        new_entries = snapshot_df.join(base_df, how="anti", on="locationId")

        joined_df = snapshot_df.join(
            base_df, on="locationId", how="left", suffix="_base", maintain_order="right"
        )
        unchanged_conditions = []
        for col_name in dataset_cols:
            unchanged_conditions.append(
                (col(f"{col_name}").eq_missing(col(f"{col_name}_base")))
            )

        rows_without_changes = joined_df.filter(unchanged_conditions)
        rows_with_changes = joined_df.remove(
            unchanged_conditions
        )  # either new rows or rows where one or more field has changed

        changed_entries = rows_with_changes.select(base_df.columns)
        unchanged_entries = rows_without_changes.select(base_df.columns)

        print(f"Removed entries: {removed_entries.shape[0]}")
        print(f"Unchanged entries: {unchanged_entries.shape[0]}")
        print(f"Changed entries: {changed_entries.shape[0]}")
        print(f"Total = {changed_entries.shape[0] + unchanged_entries.shape[0]}")

        assert (
            changed_entries.shape[0]
            + unchanged_entries.shape[0]
            - new_entries.shape[0]
            + removed_entries.shape[0]
            == base_df.shape[0]
        )

        print(
            f"Unique Last Updated before join: {base_df['last_updated'].value_counts()}"
        )

        changed_entries = changed_entries.with_columns(
            pl.lit(f"2013{month:02}01").alias("last_updated"),
            pl.lit(False).alias("db_delete"),
        )
        removed_entries = removed_entries.with_columns(
            pl.lit(f"2013{month:02}01").alias("last_updated"),
            pl.lit(True).alias("db_delete"),
        )
        base_df = concat([changed_entries, unchanged_entries])

        changed_entries = concat([changed_entries, removed_entries])

        print(
            f"Unique Last Updated after join: {base_df['last_updated'].value_counts()}"
        )
        print(f"Unique db_delete after join: {base_df['db_delete'].value_counts()}")

        changed_entries.write_parquet(
            f"s3://{write_bucket}/{write_folder}/",
            compression="snappy",
            use_pyarrow=True,
            pyarrow_options={"partition_cols": ["last_updated"]},
            partition_chunk_size_bytes=220000,
        )


if __name__ == "__main__":
    main()
