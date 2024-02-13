import sys

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

from utils import utils
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys,
    AscwdsWorkplaceColumns as AWP,
)


def main(source: str, destination: str):
    ascwds_workplace_df = utils.read_from_parquet(source)

    ascwds_workplace_df = cast_to_int(
        ascwds_workplace_df, [AWP.total_staff, AWP.worker_records]
    )

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(
        ascwds_workplace_df,
        destination,
        True,
        [
            PartitionKeys.year,
            PartitionKeys.month,
            PartitionKeys.day,
            PartitionKeys.import_date,
        ],
    )


def cast_to_int(df: DataFrame, column_names: list) -> DataFrame:
    for column in column_names:
        df = df.withColumn(column, df[column].cast(IntegerType()))
    return df


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_workplace_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--ascwds_workplace_source",
            "Source s3 directory for parquet ascwds workplace dataset",
        ),
        (
            "--ascwds_workplace_destination",
            "Destination s3 directory for cleaned parquet ascwds workplace dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
