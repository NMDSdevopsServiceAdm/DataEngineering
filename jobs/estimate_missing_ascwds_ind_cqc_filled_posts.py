import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.estimate_filled_posts.models.interpolation import model_interpolation


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    estimated_missing_ascwds_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

    cleaned_ind_cqc_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )

    cleaned_ind_cqc_df = model_interpolation(cleaned_ind_cqc_df)

    print(f"Exporting as parquet to {estimated_missing_ascwds_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_missing_ascwds_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--estimated_missing_ascwds_ind_cqc_destination",
            "Destination s3 directory for outputting estimates for filled posts",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    )
