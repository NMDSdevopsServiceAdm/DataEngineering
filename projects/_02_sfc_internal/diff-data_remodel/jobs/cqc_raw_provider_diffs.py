from polars import read_parquet, concat, col
import polars as pl
from polars.testing import assert_frame_equal

from diff_creator import get_diffs
from raw_providers_schema import raw_providers_schema


def main():
    dataset_cols = [
        "locationIds",
        "organisationType",
        "ownershipType",
        "type",
        "name",
        "brandId",
        "brandName",
        "odsCode",
        "registrationStatus",
        "registrationDate",
        "companiesHouseNumber",
        "charityNumber",
        "website",
        "postalAddressLine1",
        "postalAddressLine2",
        "postalAddressTownCity",
        "postalAddressCounty",
        "region",
        "postalCode",
        "alsoKnownAs",
        "deregistrationDate",
        "uprn",
        "onspdLatitude",
        "onspdLongitude",
        "onspdIcbCode",
        "onspdIcbName",
        "inspectionDirectorate",
        "constituency",
        "localAuthority",
        "lastInspection",
        "lastReport",
        "contacts",
        "relationships",
        "regulatedActivities",
        "inspectionCategories",
        "inspectionAreas",
        "currentRatings",
        "historicRatings",
        "reports",
        "unpublishedReports",
    ]

    bucket = "sfc-main-datasets"
    write_bucket = "sfc-test-diff-datasets"
    read_folder = Rf"domain=CQC/dataset=providers_api/version=2.0.0/year=2013/month=03/day=01/import_date=20130301/"
    write_folder = Rf"domain=CQC/dataset=providers_api/year=2013/month=03/day=01/import_date=20130301/file.parquet"

    base_df = read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_providers_schema,
    )

    base_df = base_df.with_columns(
        pl.lit("20130301").alias("last_updated"),
    )
    base_df.write_parquet(
        f"s3://{write_bucket}/{write_folder}/",
        compression="snappy",
        partition_chunk_size_bytes=220000,
    )

    print(f"Original entries: {base_df.shape}")

    for month in range(4, 12):
        print(f"Starting month {month}")

        write_folder = Rf"domain=CQC/dataset=providers_api/year=2013/month={month:02}/day=01/import_date=2013{month:02}01/"
        read_folder = Rf"domain=CQC/dataset=providers_api/version=2.0.0/year=2013/month={month:02}/day=01/import_date=2013{month:02}01/"

        print(f"Reading from {read_folder}")
        snapshot_df = read_parquet(
            f"s3://{bucket}/{read_folder}",
            schema=raw_providers_schema,
        )

        print(
            f"Unique Last Updated before join: {base_df['last_updated'].value_counts()}"
        )

        base_df, changed_entries = get_diffs(
            base_df,
            snapshot_df,
            snapshot_date=f"2013{month:02}01",
            primary_key="providerId",
            change_cols=dataset_cols,
        )

        old_entries = base_df.filter(
            col("providerId").is_in(changed_entries.head()["providerId"].to_list())
        )

        print(
            assert_frame_equal(
                changed_entries[dataset_cols].head(), old_entries[dataset_cols]
            )
        )

        print(
            f"Unique Last Updated after join: {base_df['last_updated'].value_counts()}"
        )

        changed_entries.write_parquet(
            f"s3://{write_bucket}/{write_folder}file.parquet",
            compression="snappy",
        )


if __name__ == "__main__":
    main()
