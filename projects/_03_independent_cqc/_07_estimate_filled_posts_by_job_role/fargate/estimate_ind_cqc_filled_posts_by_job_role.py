import logging
import sys

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

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

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
prepared_ascwds_job_role_counts_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.ascwds_job_role_counts,
]


def main(
    estimated_ind_cqc_filled_posts_source: str,
    prepared_ascwds_job_role_counts_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_ind_cqc_filled_posts_source (str): path to the estimates ind cqc filled posts data
        prepared_ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        estimated_ind_cqc_filled_posts_by_job_role_destination (str): destination for output
    """
    estimated_ind_cqc_filled_posts_lf = utils.scan_parquet(
        source=estimated_ind_cqc_filled_posts_source,
        selected_columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )
    prepared_ascwds_job_role_counts_lf = utils.scan_parquet(
        source=prepared_ascwds_job_role_counts_source,
        selected_columns=prepared_ascwds_job_role_counts_columns_to_import,
    )

    estimated_ind_cqc_filled_posts_by_job_role_lf = (
        JRUtils.join_worker_to_estimates_dataframe(
            estimated_ind_cqc_filled_posts_lf, prepared_ascwds_job_role_counts_lf
        )
    )

    utils.sink_to_parquet(
        lazy_df=estimated_ind_cqc_filled_posts_by_job_role_lf,
        output_path=estimated_ind_cqc_filled_posts_by_job_role_destination,
        partition_cols=partition_keys,
        logger=logger,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--prepared_ascwds_job_role_counts_source",
            "Source s3 directory for parquet ASCWDS worker job role counts dataset",
        ),
        (
            "--estimated_ind_cqc_filled_posts_by_job_role_destination",
            "Destination s3 directory",
        ),
    )
    main(
        estimated_ind_cqc_filled_posts_source=args.estimated_ind_cqc_filled_posts_source,
        prepared_ascwds_job_role_counts_source=args.prepared_ascwds_job_role_counts_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination=args.estimated_ind_cqc_filled_posts_by_job_role_destination,
    )
