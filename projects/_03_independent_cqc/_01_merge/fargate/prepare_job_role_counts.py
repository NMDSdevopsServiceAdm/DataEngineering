import logging
import sys

import polars as pl

import projects._03_independent_cqc._01_merge.fargate.utils.utils as JRUtils
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
    cleaned_ascwds_worker_source: str,
    prepared_ascwds_job_role_counts_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        prepared_ascwds_job_role_counts_destination (str): destination for output.
    """
    cleaned_ascwds_worker_lf = pl.scan_parquet(
        cleaned_ascwds_worker_source,
    ).select(cleaned_ascwds_worker_columns_to_import)

    aggregated_worker_lf = JRUtils.aggregate_ascwds_worker_job_roles_per_establishment(
        cleaned_ascwds_worker_lf,
        JRUtils.LIST_OF_JOB_ROLES_SORTED,
    )

    sink_parquet_with_partitions(
        aggregated_worker_lf,
        prepared_ascwds_job_role_counts_destination,
    )


def sink_parquet_with_partitions(
    lf: pl.LazyFrame, prepared_ascwds_job_role_counts_destination: str
) -> None:
    path = pl.PartitionByKey(
        base_path=f"{prepared_ascwds_job_role_counts_destination}",
        include_key=False,
        by=partition_keys,
    )

    lf.sink_parquet(path=path, mkdir=True, engine="streaming")


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for parquet ASCWDS worker cleaned dataset",
        ),
        (
            "--prepared_ascwds_job_role_counts_destination",
            "Destination s3 directory for prepared_ascwds_job_role_counts",
        ),
    )

    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        prepared_ascwds_job_role_counts_destination=args.estimated_ind_cqc_filled_posts_by_job_role_destination,
    )

    logger.info("Finished preparing ascwds job role counts")
