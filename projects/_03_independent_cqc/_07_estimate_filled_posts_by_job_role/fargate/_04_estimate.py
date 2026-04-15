from typing import Final

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.estimate_utils as eUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Define constants for IDs for original length data.
ROW_ID: Final[str] = "id"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

# Create a list of non registered manager managerial job roles.
non_rm_manager_roles = [
    role
    for role in AscwdsWorkerValueLabelsJobGroup.manager_roles()
    if role != MainJobRoleLabels.registered_manager
]


def main(
    imputed_data_source: str,
    estimated_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        imputed_data_source (str): path to the imputed data
        estimated_data_destination (str): destination for output
    """
    print("Estimates Job Started...")

    lf = utils.scan_parquet(imputed_data_source)

    lf = eUtils.calculate_estimated_filled_posts_by_job_role(lf)

    lf = lf.with_columns(
        eUtils.has_rm_in_cqc_rm_name_list_flag().alias(IndCQC.registered_manager_count)
    )

    lf = eUtils.adjust_managerial_roles(lf, non_rm_manager_roles)

    lf = lf.with_columns(
        pl.col(IndCQC.cqc_location_import_date).dt.year().alias(Keys.year)
    )

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=estimated_data_destination,
        partition_cols=[Keys.year],
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--imputed_data_source",
            "Source s3 directory for imputed data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimates by job role",
        ),
    )
    main(
        imputed_data_source=args.imputed_data_source,
        estimated_data_destination=args.estimated_data_destination,
    )
