import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


cleaned_ind_cqc_columns = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.number_of_beds,
    IndCQC.cqc_pir_import_date,
    IndCQC.people_directly_employed,
    IndCQC.people_directly_employed_dedup,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    estimated_non_ml_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating independent CQC filled posts...")

    spark = utils.get_spark()

    cleaned_ind_cqc_df = utils.read_from_parquet(
        cleaned_ind_cqc_source, cleaned_ind_cqc_columns
    )

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
