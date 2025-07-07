from polars import read_parquet, concat, col
import polars as pl
from polars.testing import assert_frame_equal

from diff_creator import get_diffs
from raw_schema import raw_schema


def main():
    dataset_cols = [
        "providerId",
        "organisationType",
        "type",
        "name",
        "brandId",
        "brandName",
        "onspdCcgCode",
        "onspdCcgName",
        "odsCcgCode",
        "odsCcgName",
        "onspdIcbCode",
        "onspdIcbName",
        "odsCode",
        "registrationStatus",
        "registrationDate",
        "deregistrationDate",
        "dormancy",
        "dormancyStartDate",
        "dormancyEndDate",
        "alsoKnownAs",
        "onspdLatitude",
        "onspdLongitude",
        "careHome",
        "inspectionDirectorate",
        "website",
        "postalAddressLine1",
        "postalAddressLine2",
        "postalAddressTownCity",
        "postalAddressCounty",
        "region",
        "postalCode",
        "uprn",
        "registeredManagerAbsentDate",
        "numberOfBeds",
        "constituency",
        "localAuthority",
        "lastInspection",
        "lastReport",
        "relationships",
        "locationTypes",
        "regulatedActivities",
        "gacServiceTypes",
        "specialisms",
        "inspectionCategories",
        "inspectionAreas",
        "currentRatings",
        "historicRatings",
        "reports",
        "unpublishedReports",
        "providerInspectionAreas",
        "specialism",
        "ageGroup",
        "settingServices",
    ]

    bucket = "sfc-main-datasets"
    write_bucket = "sfc-test-diff-datasets"
    read_folder = Rf"domain=CQC/dataset=locations_api/version=2.1.1/year=2013/month=03/day=01/import_date=20130301/"
    write_folder = Rf"domain=CQC/dataset=locations_api/year=2013/month=03/import_date=20130301/file.parquet"

    base_df = read_parquet(
        f"s3://{bucket}/{read_folder}",
        schema=raw_schema,
    )
    print(base_df.schema)

    base_df = base_df.with_columns(
        pl.lit("20130301").alias("last_updated"),
        pl.lit(False).alias("db_delete"),
    )
    # base_df.write_parquet(
    #     f"s3://{write_bucket}/{write_folder}/",
    #     compression="snappy",
    #     partition_chunk_size_bytes=220000,
    # )

    # print(f"Original entries: {base_df.shape}")

    # todo: seems like a high rate of change - have a look and ensure this is genuine
    # todo: in checks, probably check for an anomalously high change rate

    for month in range(4, 12):
        print(f"Starting month {month}")

        write_folder = Rf"domain=CQC/dataset=locations_api/year=2013/month={month:02}/import_date=2013{month:02}01/file.parquet"
        read_folder = Rf"domain=CQC/dataset=locations_api/version=2.1.1/year=2013/month={month:02}/day=01/import_date=2013{month:02}01/"

        print(f"Reading from {read_folder}")
        snapshot_df = read_parquet(
            f"s3://{bucket}/{read_folder}",
            schema=raw_schema,
        )

        print(
            f"Unique Last Updated before join: {base_df['last_updated'].value_counts()}"
        )

        base_df, changed_entries = get_diffs(
            base_df,
            snapshot_df,
            snapshot_date=f"2013{month:02}01",
            primary_key="locationId",
            change_cols=dataset_cols,
        )

        old_entries = base_df.filter(
            col("locationId").is_in(changed_entries.head()["locationId"].to_list())
        )

        print(
            assert_frame_equal(
                changed_entries[dataset_cols].head(), old_entries[dataset_cols]
            )
        )

        print(changed_entries[dataset_cols].head())
        print(old_entries[dataset_cols])

        print(
            f"Unique Last Updated after join: {base_df['last_updated'].value_counts()}"
        )

        # changed_entries.write_parquet(
        #     f"s3://{write_bucket}/{write_folder}/",
        #     compression="snappy",
        #     use_pyarrow=True,
        #     pyarrow_options={"partition_cols": ["last_updated"]},
        #     partition_chunk_size_bytes=220000,
        # )


if __name__ == "__main__":
    main()
