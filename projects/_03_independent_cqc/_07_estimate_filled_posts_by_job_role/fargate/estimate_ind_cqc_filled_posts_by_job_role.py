import logging
import sys

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as JRUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

cleaned_ascwds_worker_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
]
estimated_ind_cqc_filled_posts_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.unix_time,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_gac_service_types,
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
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    estimated_ind_cqc_filled_posts_source: str,
    cleaned_ascwds_worker_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_ind_cqc_filled_posts_source (str): path to the estimates ind cqc filled posts data
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        estimated_ind_cqc_filled_posts_by_job_role_destination (str): path to where to save the outputs
    """
    estimated_ind_cqc_filled_posts_lf = pl.scan_parquet(
        estimated_ind_cqc_filled_posts_source,
    ).select(estimated_ind_cqc_filled_posts_columns_to_import)

    cleaned_ascwds_worker_lf = pl.scan_parquet(
        cleaned_ascwds_worker_source,
    ).select(cleaned_ascwds_worker_columns_to_import)

    aggregated_worker_lf = JRUtils.aggregate_ascwds_worker_job_roles_per_establishment(
        cleaned_ascwds_worker_lf, JRUtils.LIST_OF_JOB_ROLES_SORTED
    )

    estimated_ind_cqc_filled_posts_by_job_role_lf = (
        JRUtils.join_worker_to_estimates_dataframe(
            estimated_ind_cqc_filled_posts_lf, aggregated_worker_lf
        )
    )

    # columns_in_output = [
    #     IndCQC.location_id,
    #     IndCQC.establishment_id,
    #     IndCQC.ascwds_workplace_import_date,
    #     IndCQC.main_job_role_clean_labelled,
    #     IndCQC.ascwds_job_role_counts,
    # ]

    estimated_ind_cqc_filled_posts_by_job_role_lf_schema = (
        estimated_ind_cqc_filled_posts_by_job_role_lf.collect_schema()
    )

    estimated_ind_cqc_filled_posts_by_job_role_lf.sink_parquet(
        path=f"{estimated_ind_cqc_filled_posts_by_job_role_destination}estimated_ind_cqc_filled_posts_by_job_role_lf.parquet",
        compression="snappy",
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for parquet ASCWDS worker cleaned dataset",
        ),
        (
            "--estimated_ind_cqc_filled_posts_by_job_role_destination",
            "Destination s3 directory",
        ),
    )

    main(
        estimated_ind_cqc_filled_posts_source=args.estimated_ind_cqc_filled_posts_source,
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination=args.estimated_ind_cqc_filled_posts_by_job_role_destination,
    )

    logger.info("Finished ind cqc estimates by job role job")
