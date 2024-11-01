import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


def main(
    estimate_ind_cqc_filled_posts_source: str,
    monthly_filled_posts_archive_destination: str,
    annual_filled_posts_archive_destination: str,
):
    print("Archiving independent CQC filled posts...")

    spark = utils.get_spark()

    estimate_filled_posts_df = utils.read_from_parquet(
        estimate_ind_cqc_filled_posts_source,
    )

    print(f"Exporting as parquet to {monthly_filled_posts_archive_destination}")

    print("Completed archive independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'archive_filled_posts_estimates' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_ind_cqc_filled_posts_source,
        monthly_filled_posts_archive_destination,
        annual_filled_posts_archive_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated_ind_cqc_filled_posts",
        ),
        (
            "--monthly_filled_posts_archive_destination",
            "Destination s3 directory for monthly filled posts estimates archive",
        ),
        (
            "--annual_filled_posts_archive_destination",
            "Destination s3 directory for annual filled posts estimates archive",
        ),
    )

    main(
        estimate_ind_cqc_filled_posts_source,
        monthly_filled_posts_archive_destination,
        annual_filled_posts_archive_destination,
    )
