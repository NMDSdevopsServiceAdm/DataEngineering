from polars import read_parquet

bucket = "sfc-main-datasets"
write_bucket = "sfc-test-diff-datasets"
read_folder = Rf"domain=CQC/dataset=locations_api_cleaned/year=2013/month=03/"

base_df = read_parquet(f"s3://{bucket}/{read_folder}")

print(base_df.columns)
print(len(base_df.columns))