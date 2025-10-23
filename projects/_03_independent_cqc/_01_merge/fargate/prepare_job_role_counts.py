import logging
import sys

import polars as pl

import projects._03_independent_cqc._01_merge.fargate.utils.utils as JRUtils
from polars_utils import cleaning_utils as CUtils
from polars_utils import utils as utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.worker_label_dictionary import (
    ascwds_worker_labels_dict,
)

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
    Prepares cleaned ASC-WDS worker data for merging with ind cqc estimates.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        prepared_ascwds_job_role_counts_destination (str): destination for output
    """
    cleaned_ascwds_worker_lf = utils.scan_parquet(
        source=cleaned_ascwds_worker_source,
        selected_columns=cleaned_ascwds_worker_columns_to_import,
    )

    aggregated_worker_lf = JRUtils.aggregate_ascwds_worker_job_roles_per_establishment(
        cleaned_ascwds_worker_lf,
        JRUtils.LIST_OF_JOB_ROLES_SORTED,
    )

    aggregated_worker_lf = JRUtils.create_job_role_ratios(aggregated_worker_lf)

    aggregated_worker_lf = CUtils.apply_categorical_labels(
        lf=aggregated_worker_lf,
        labels=ascwds_worker_labels_dict,
        columns_to_apply=[IndCQC.main_job_role_clean_labelled],
        add_as_new_column=True,
        customise_new_column_names=[IndCQC.main_job_group_labelled],
    )

    utils.sink_to_parquet(
        lazy_df=aggregated_worker_lf,
        output_path=prepared_ascwds_job_role_counts_destination,
        partition_cols=partition_keys,
        logger=logger,
        append=False,
    )


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
        prepared_ascwds_job_role_counts_destination=args.prepared_ascwds_job_role_counts_destination,
    )

    logger.info("Finished preparing ascwds job role counts")
