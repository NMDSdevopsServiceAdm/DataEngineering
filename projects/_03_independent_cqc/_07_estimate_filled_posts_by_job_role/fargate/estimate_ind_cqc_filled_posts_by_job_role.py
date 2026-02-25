import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as JRUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

estimates_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.unix_time,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_registration_date,
    IndCQC.registered_manager_names,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
ascwds_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.ascwds_job_role_counts,
]


def main(
    estimates_source: str,
    ascwds_job_role_counts_source: str,
    estimates_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        estimates_by_job_role_destination (str): destination for output
    """
    estimated_posts_lf = utils.scan_parquet(
        source=estimates_source,
        selected_columns=estimates_columns_to_import,
    )
    ascwds_job_role_counts_lf = utils.scan_parquet(
        source=ascwds_job_role_counts_source,
        selected_columns=ascwds_columns_to_import,
    )

    estimated_job_role_posts_lf = JRUtils.join_worker_to_estimates_dataframe(
        estimated_posts_lf, ascwds_job_role_counts_lf
    )

    estimated_job_role_posts_lf = JRUtils.nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        JRUtils.get_percentage_share(IndCQC.ascwds_job_role_counts)
        .over(IndCQC.location_id)
        .alias(IndCQC.ascwds_job_role_ratios)
    )
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.ascwds_job_role_ratios)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .over(
            IndCQC.location_id,
            IndCQC.main_job_role_clean_labelled,
            order_by=IndCQC.unix_time,
        )
        .alias(IndCQC.ascwds_job_role_ratios_interpolated)
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=estimates_by_job_role_destination,
        partition_cols=partition_keys,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--ascwds_job_role_counts_source",
            "Source s3 directory for parquet ASCWDS worker job role counts dataset",
        ),
        (
            "--estimates_by_job_role_destination",
            "Destination s3 directory",
        ),
    )
    main(
        estimates_source=args.estimates_source,
        ascwds_job_role_counts_source=args.ascwds_job_role_counts_source,
        estimates_by_job_role_destination=args.estimates_by_job_role_destination,
    )
