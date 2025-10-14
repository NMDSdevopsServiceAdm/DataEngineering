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

    logger.info("Finished joining lazyframes")

    # estimated_ind_cqc_filled_posts_by_job_role_lf = (
    #     JRUtils.join_worker_to_estimates_dataframe(
    #         estimated_ind_cqc_filled_posts_lf, aggregated_worker_lf
    #     )
    # )

    # plan = estimated_ind_cqc_filled_posts_lf.explain(
    #     engine="streaming",
    # )
    # logger.info(plan)

    # total_rows = 0
    # for i in range(2013, 2025, 1):
    #     rows_in_partition = (
    #         aggregated_worker_lf.filter(pl.col(Keys.year) == i)
    #         .select(pl.len())
    #         .collect(engine="streaming")
    #         .item()
    #     )
    #     logger.info(f"Rows in partition {i}: {rows_in_partition}")
    #     total_rows += rows_in_partition

    total_rows = (
        aggregated_worker_lf.select(pl.len()).collect(engine="streaming").item()
    )

    logger.info(f"Total rows: {total_rows}")

    # batch_size = pl.lit(100000, pl.Int64())
    # for i in range(0, total_rows, batch_size):
    #     aggregated_worker_lf.slice(i, batch_size).sink_parquet(
    #         path=f"{estimated_ind_cqc_filled_posts_by_job_role_destination}estimated_ind_cqc_filled_posts_by_job_role_lf-{i}.parquet",
    #         compression="snappy",
    #     )

    #     logger.info(f"Wrote batch {i} of {total_rows}")

    # utils.write_to_parquet(
    #     aggregated_worker_lf.collect(engine="streaming"),
    #     f"{estimated_ind_cqc_filled_posts_by_job_role_destination}estimated_ind_cqc_filled_posts_by_job_role_lf.parquet",
    #     logger=logger,
    #     append=False,
    # )


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
