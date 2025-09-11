import logging
import sys
from argparse import ArgumentError, ArgumentTypeError

import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

# Test commit.

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
    estimated_ind_cqc_filled_posts_df = pl.read_parquet(
        source=estimated_ind_cqc_filled_posts_source,
        columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )

    cleaned_ascwds_worker_df = pl.read_parquet(
        source=cleaned_ascwds_worker_source,
        columns=cleaned_ascwds_worker_columns_to_import,
    )

    utils.write_to_parquet(
        estimated_ind_cqc_filled_posts_df,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
        logger=logger,
    )


if __name__ == "__main__":
    try:
        (
            estimated_ind_cqc_filled_posts_source,
            cleaned_ascwds_worker_source,
            estimated_ind_cqc_filled_posts_by_job_role_destination,
            *_,
        ) = utils.collect_arguments(
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
            estimated_ind_cqc_filled_posts_source,
            cleaned_ascwds_worker_source,
            estimated_ind_cqc_filled_posts_by_job_role_destination,
        )

    except (ArgumentError, ArgumentTypeError) as e:
        logger.error(f"An error occurred parsing arguments for {sys.argv}")
        raise e
