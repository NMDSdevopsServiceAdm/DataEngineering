from typing import Final

import polars as pl

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


def main(
    estimated_row_data_source: str,
    estimated_column_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_row_data_source (str): path to the estimates by job role as rows data
        estimated_column_data_destination (str): destination for estimates by job role as columns data
    """
    print("Pivot Job Started...")

    lf = utils.scan_parquet(estimated_row_data_source)

    lf = pivot_job_role_rows_to_columns(lf, AscwdsWorkerValueLabelsJobGroup.all_roles())

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=estimated_column_data_destination,
        partition_cols=[Keys.year],
        append=False,
    )


def pivot_job_role_rows_to_columns(
    lf: pl.LazyFrame, job_roles_to_become_columns: list
) -> pl.LazyFrame:
    "Doc string here"

    lf = lf.select(
        [
            ROW_ID,
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
            IndCQC.primary_service_type_second_level,
            IndCQC.current_cssr,
            IndCQC.current_region,
            IndCQC.contemporary_cssr,
            IndCQC.contemporary_region,
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.ascwds_job_role_ratios_merged_source,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted,
            Keys.year,
        ]
    )

    cols_for_pivotting = [
        ROW_ID,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.estimate_filled_posts_by_job_role_manager_adjusted,
    ]
    lf_pivoted = lf.select(cols_for_pivotting).pivot(
        on=IndCQC.main_job_role_clean_labelled,
        on_columns=job_roles_to_become_columns,
        index=ROW_ID,
        values=IndCQC.estimate_filled_posts_by_job_role_manager_adjusted,
    )

    lf = lf.unique(ROW_ID).drop(
        [
            IndCQC.main_job_role_clean_labelled,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted,
        ]
    )

    lf = lf.join(lf_pivoted, on=ROW_ID)

    return lf


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimated_row_data_source",
            "Source s3 directory for estimates by job role per row data",
        ),
        (
            "--estimated_column_data_destination",
            "Destination s3 directory for estimates by job role as columns data",
        ),
    )
    main(
        estimated_row_data_source=args.estimated_row_data_source,
        estimated_column_data_destination=args.estimated_column_data_destination,
    )
