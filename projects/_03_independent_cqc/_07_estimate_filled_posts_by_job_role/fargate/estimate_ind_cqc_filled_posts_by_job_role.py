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

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
cleaned_ascwds_worker_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
estimated_ind_cqc_filled_posts_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.estimate_filled_posts,
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

    # logger.info("Finished aggregating worker data. Printing query plan.")

    # estimated_ind_cqc_filled_posts_by_job_role_lf = (
    #     JRUtils.join_worker_to_estimates_dataframe(
    #         estimated_ind_cqc_filled_posts_lf, aggregated_worker_lf[0]
    #     )
    # )

    # logger.info("Finished joing worker data to estimates")

    sink_parquet_with_partitions(
        aggregated_worker_lf[0],
        "s3://sfc-1032-jb-rle-est-prs-datasets/domain=ind_cqc_filled_posts/dataset=temp_folder_estimates/temp_folder_1",
    )

    sink_parquet_with_partitions(
        aggregated_worker_lf[1],
        "s3://sfc-1032-jb-rle-est-prs-datasets/domain=ind_cqc_filled_posts/dataset=temp_folder_estimates/temp_folder_2",
    )

    # unique_years_list = get_unique_years_as_list(
    #     estimated_ind_cqc_filled_posts_by_job_role_lf
    # )

    # unique_years_list = [
    #     2013,
    #     2014,
    #     2015,
    #     2016,
    #     2017,
    #     2018,
    #     2019,
    #     2020,
    #     2021,
    #     2022,
    #     2023,
    #     2024,
    #     2025,
    # ]

    # logger.info("Finished getting unqiue list of year to loop through")

    # logger.info("Starting to sink parquet via loop")

    # for i in unique_years_list:
    #     batch = estimated_ind_cqc_filled_posts_by_job_role_lf.filter(
    #         pl.col(Keys.year) == i
    #     )

    #     sink_parquet_with_partitions(
    #         batch,
    #         estimated_ind_cqc_filled_posts_by_job_role_destination,
    #     )

    #     logger.info(f"Finished sinking year: {i}")


def sink_parquet_with_partitions(
    lf: pl.LazyFrame, estimated_ind_cqc_filled_posts_by_job_role_destination: str
) -> None:
    path = pl.PartitionByKey(
        base_path=f"{estimated_ind_cqc_filled_posts_by_job_role_destination}",
        include_key=False,
        by=partition_keys,
    )

    lf.sink_parquet(path=path, mkdir=True, engine="streaming")


def get_unique_years_as_list(lf: pl.LazyFrame) -> list:
    return (
        lf.select(Keys.year).unique().collect(engine="streaming").to_series().to_list()
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
