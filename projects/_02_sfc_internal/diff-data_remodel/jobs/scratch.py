from polars import read_parquet

bucket = "sfc-main-datasets"
read_folder = Rf"domain=CQC/dataset=locations_api/version=2.1.1/year=2013/"

base_df = read_parquet(f"s3://{bucket}/{read_folder}")

print(base_df.columns)
print(len(base_df.columns))
