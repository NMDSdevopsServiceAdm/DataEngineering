from polars import read_parquet, concat, col
import polars as pl
from polars.testing import assert_frame_equal

from utils import download_from_s3, list_bucket_objects

def main ():
    # for filename in list_bucket_objects(bucket="sfc-main-datasets",
    #                                     prefix=R"domain=CQC/dataset=locations_api_cleaned/year=2013/month=03/day=01/import_date=20130301/"):
    #     partition_name = filename[filename.rfind("/")+1:]
    #     download_from_s3(src_bucket="sfc-main-datasets",
    #                      src_filename=filename,
    #                      dest_filename=f"base_dataset/{partition_name}",)

    # base_df = read_parquet(f"base_dataset")

    dataset_cols = ['postalCode', 'providerId',
                    'careHome', 'dormancy', 'gacServiceTypes', 'name',
                    'numberOfBeds', 'registrationStatus', 'registrationDate', 'deregistrationDate', 'regulatedActivities',
                    'specialisms', 'type', 'relationships', 'imputed_registration_date',
                    'imputed_relationships', 'imputed_gacServiceTypes', 'imputed_regulatedActivities',
                    'imputed_specialisms', 'services_offered', 'specialisms_offered', 'primary_service_type',
                    'registered_manager_names', 'related_location', 'provider_name', 'cqc_sector', 'contemporary_CSSR',
                    'contemporary_Region', 'contemporary_Sub_ICB', 'contemporary_ICB', 'contemporary_ICB_Region',
                    'contemporary_CCG', 'contemporary_lat', 'contemporary_long', 'contemporary_imd', 'contemporary_lsoa11',
                    'contemporary_msoa11', 'contemporary_ru11ind', 'contemporary_lsoa21', 'contemporary_msoa21',
                    'contemporary_pcon', 'current_CSSR', 'current_Region', 'current_Sub_ICB',
                    'current_ICB', 'current_ICB_Region', 'current_CCG', 'current_lat', 'current_long', 'current_imd',
                    'current_lsoa11', 'current_msoa11', 'current_ru11ind', 'current_lsoa21', 'current_msoa21',
                    'current_pcon']

    bucket = "sfc-main-datasets"
    folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month=03/day=01/import_date=20130301/"

    base_df = read_parquet(f"s3://{bucket}/{folder}")
    print(f"Original entries: {base_df.shape}")

    for month in range(4,5):
        print(f"Starting month {month}")

        folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month={month:02}/day=01/import_date=2013{month:02}01/"
        print(f"Reading from {folder}")
        snapshot_df = read_parquet(f"s3://{bucket}/{folder}")

        removed_entries = base_df.join(snapshot_df, how="anti", on="locationId")
        print(f"Removed Location IDs: {removed_entries.shape[0]}")

        new_entries = snapshot_df.join(base_df, how="anti", on="locationId")
        print(f"New Location IDs: {new_entries.shape[0]}")
        # base_df = concat([base_df, new_entries])

        joined_df = snapshot_df.join(base_df, on="locationId", how="left", suffix="_base", maintain_order="right")
        unchanged_conditions = []
        for col_name in dataset_cols:
            unchanged_conditions.append(
                (col(f"{col_name}").eq_missing(col(f"{col_name}_base")))
            )

        rows_without_changes = joined_df.filter(unchanged_conditions)
        rows_with_changes = joined_df.remove(unchanged_conditions)

        changed_entries = rows_with_changes.select(snapshot_df.columns)
        unchanged_entries = rows_without_changes.select(snapshot_df.columns)

        print(f"Joined df: {joined_df.shape[0]}")
        print(f"Unchanged entries: {unchanged_entries.shape[0]}")
        print(f"Changed entries: {changed_entries.shape[0]}")
        print(f"Total = {changed_entries.shape[0] + unchanged_entries.shape[0]}")

        # old_entries = base_df.filter(col("locationId").is_in(changed_entries.head()["locationId"].to_list()))

        # print(assert_frame_equal(changed_entries[dataset_cols].head(), old_entries[dataset_cols]))
        # base_df = concat([base_df, new_entries])
        assert(changed_entries.shape[0]+unchanged_entries.shape[0]-new_entries.shape[0]+removed_entries.shape[0]==base_df.shape[0])


if __name__ == "__main__":
    main()