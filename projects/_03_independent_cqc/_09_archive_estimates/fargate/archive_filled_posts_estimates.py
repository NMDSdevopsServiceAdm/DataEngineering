from datetime import datetime

import projects._03_independent_cqc._09_archive_estimates.fargate.utils.archive_utils as aUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

MONTHLY_ARCHIVE_COLUMNS = [
    IndCQC.ascwds_filled_posts,
    IndCQC.ascwds_filled_posts_dedup,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filled_posts_source,
    IndCQC.ascwds_filtering_rule,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.care_home,
    IndCQC.care_home_model,
    IndCQC.contemporary_ccg,
    IndCQC.contemporary_cssr,
    IndCQC.contemporary_icb,
    IndCQC.contemporary_icb_region,
    IndCQC.contemporary_region,
    IndCQC.contemporary_sub_icb,
    IndCQC.cqc_location_import_date,
    IndCQC.cqc_pir_import_date,
    IndCQC.current_cssr,
    IndCQC.current_icb,
    IndCQC.current_region,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.dormancy,
    IndCQC.establishment_id,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    IndCQC.imputed_pir_filled_posts_model,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_combined_model,
    IndCQC.imputed_filled_post_model,
    IndCQC.imputed_filled_posts_per_bed_ratio_model,
    IndCQC.imputed_registration_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.non_res_combined_model,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.number_of_beds,
    IndCQC.organisation_id,
    IndCQC.pir_people_directly_employed_dedup,
    IndCQC.pir_filled_posts_model,
    IndCQC.posts_rolling_average_model,
    IndCQC.primary_service_type,
    IndCQC.primary_service_type_second_level,
    IndCQC.provider_id,
    IndCQC.regulated_activities_offered,
    IndCQC.related_location,
    IndCQC.specialisms_offered,
    IndCQC.total_staff_bounded,
    IndCQC.worker_records_bounded,
]
partition_keys = [
    ArchiveKeys.archive_year,
    ArchiveKeys.archive_month,
    ArchiveKeys.archive_day,
    ArchiveKeys.archive_timestamp,
]


def main(
    estimate_ind_cqc_filled_posts_source: str,
    archive_ind_cqc_filled_posts_destination: str,
):
    print("Archiving independent CQC filled posts...")

    estimate_filled_posts_lf = utils.scan_parquet(
        estimate_ind_cqc_filled_posts_source,
        selected_columns=MONTHLY_ARCHIVE_COLUMNS,
    )

    archive_lf = aUtils.select_import_dates_to_archive(estimate_filled_posts_lf)

    timestamp = datetime.now()
    archive_lf = aUtils.create_archive_date_partition_columns(archive_lf, timestamp)

    print(f"Exporting as parquet to {archive_ind_cqc_filled_posts_destination}")

    utils.sink_to_parquet(
        archive_lf,
        archive_ind_cqc_filled_posts_destination,
        partition_cols=partition_keys,
        append=False,
    )

    print("Completed archive independent CQC filled posts")


if __name__ == "__main__":
    print("Running Archive Independent CQC job")

    args = utils.get_args(
        (
            "--estimate_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated_ind_cqc_filled_posts",
        ),
        (
            "--archive_ind_cqc_filled_posts_destination",
            "S3 URI to append archive data to",
        ),
    )

    main(
        estimate_ind_cqc_filled_posts_source=args.estimate_ind_cqc_filled_posts_source,
        archive_ind_cqc_filled_posts_destination=args.archive_ind_cqc_filled_posts_destination,
    )

    print("Finished Archive Independent CQC job")
