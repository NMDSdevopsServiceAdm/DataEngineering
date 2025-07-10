from polars import read_parquet, col
from polars.testing import assert_frame_equal

from utils import build_full_table_from_delta
from raw_providers_schema import raw_providers_schema

full_from_delta = (
    build_full_table_from_delta(
        bucket="sfc-test-diff-datasets",
        read_folder="domain=CQC/dataset=providers_api/year=2013/",
    )
    .drop(["last_updated", "mainPhoneNumber"])
    .remove(col("deregistrationDate").ne(""))
)

original_format = read_parquet(
    "s3://sfc-main-datasets/domain=CQC/dataset=providers_api/version=2.0.0/year=2013/",
    schema=raw_providers_schema,
).drop(["mainPhoneNumber"])


assert_frame_equal(
    full_from_delta,
    original_format,
    check_row_order=False,
)

# from polars import read_parquet
#
# base_df = read_parquet(
#     f"s3://sfc-main-datasets/domain=CQC/dataset=providers_api/version=2.0.0/year=2013/"
# )
#
# print(base_df.columns)
# print(base_df.schema)
# print(len(base_df.columns))
# print(base_df.head())
