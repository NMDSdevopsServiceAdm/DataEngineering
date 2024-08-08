import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    estimated_non_ml_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    spark = utils.get_spark()

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

    print(f"Exporting as parquet to {estimated_non_ml_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_non_ml_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        estimated_non_ml_ind_cqc_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--estimated_non_ml_ind_cqc_destination",
            "Destination s3 directory for outputting estimates for filled posts",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        estimated_non_ml_ind_cqc_destination,
    )
