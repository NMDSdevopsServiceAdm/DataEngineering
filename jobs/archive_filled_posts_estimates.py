import sys

from pyspark.sql import DataFrame

from utils import utils, cleaning_utils as CUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)

MONTHLY_ARCHIVE_COLUMNS = [
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filtering_rule,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.care_home,
    IndCQC.care_home_model,
    IndCQC.cqc_location_import_date,
    IndCQC.cqc_pir_import_date,
    IndCQC.current_cssr,
    IndCQC.current_icb,
    IndCQC.current_region,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.dormancy,
    IndCQC.establishment_id,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    IndCQC.imputed_gac_service_types,
    IndCQC.imputed_non_res_people_directly_employed,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_with_dormancy_model,
    IndCQC.imputed_posts_rolling_avg_model,
    IndCQC.imputed_ratio_rolling_avg_model,
    IndCQC.imputed_registration_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.number_of_beds,
    IndCQC.organisation_id,
    IndCQC.people_directly_employed_dedup,
    IndCQC.primary_service_type,
    IndCQC.provider_id,
    IndCQC.provider_name,
    IndCQC.related_location,
    IndCQC.rolling_average_model_filled_posts_per_bed_ratio,
    IndCQC.rolling_average_model,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
]


def main(
    estimate_ind_cqc_filled_posts_source: str,
    monthly_filled_posts_archive_destination: str,
    annual_filled_posts_archive_destination: str,
):
    print("Archiving independent CQC filled posts...")

    spark = utils.get_spark()

    estimate_filled_posts_df = utils.read_from_parquet(
        estimate_ind_cqc_filled_posts_source,
        MONTHLY_ARCHIVE_COLUMNS,
    )

    monthly_estimates_df = utils.filter_df_to_maximum_value_in_column(
        estimate_filled_posts_df, IndCQC.cqc_location_import_date
    )

    monthly_estimates_df = create_archive_date_partition_columns(
        monthly_estimates_df, IndCQC.cqc_location_import_date
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
