"""THROWAWAY. Instrumented copy of _04_estimate.py for the 2102 monthly-data spike.

Mirrors _04_estimate.main exactly, with a RunDiagnostics checkpoint either side of each
step, so the RSS curve can be attributed to a stage rather than to the job as a whole.
The open question is whether retaining all historical data at monthly resolution, plus
dropping job role rows whose final estimate is exactly zero, pushes peak memory near
the task's ceiling.

Writes to its own dataset name so it can never overwrite the real pipeline's output.
Delete this file, its Dockerfile COPY line, its terraform module and its step function
definition once the investigation concludes.
"""

import polars as pl

import projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate.utils.estimate_utils as eUtils
from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from polars_utils.utils import split_s3_uri
from projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate.utils.utils import (
    add_job_role_groups_column,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

SAMPLE_INTERVAL_SECONDS: float = 10

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
    Instrumented copy of the job role estimates estimate step.

    Args:
        imputed_data_source (str): path to the imputed data
        estimated_data_destination (str): destination for output
    """
    data_bucket, _ = split_s3_uri(estimated_data_destination)
    diagnostics = RunDiagnostics(
        "job_role_estimates_04_estimate_prototype",
        data_bucket,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        lf = utils.scan_parquet(imputed_data_source)
        diagnostics.checkpoint("after_scan", lf)

        lf = eUtils.calculate_estimated_filled_posts_by_job_role(lf)
        diagnostics.checkpoint("after_calculate_estimates", lf)

        lf = lf.with_columns(
            eUtils.has_rm_in_cqc_rm_name_list_flag().alias(
                IndCQC.registered_manager_count
            )
        )

        lf = eUtils.adjust_managerial_roles(lf, non_rm_manager_roles)
        diagnostics.checkpoint("after_adjust_managerial_roles", lf)

        lf = eUtils.reallocate_historical_filled_posts_by_job_role(lf)
        diagnostics.checkpoint("after_reallocate_historical", lf)

        # A job role row with an exactly-zero final estimate has no posts to split
        # into further breakdowns (e.g. employment status, gender) downstream.
        zero_estimate_counts = lf.select(
            pl.len().alias("total_rows"),
            (pl.col(IndCQC.estimate_filled_posts_by_job_role) == 0.0)
            .sum()
            .alias("zero_estimate_rows"),
        ).collect()
        print(
            f"Dropping {zero_estimate_counts['zero_estimate_rows'][0]} of "
            f"{zero_estimate_counts['total_rows'][0]} rows with an exactly-zero "
            "final job role estimate"
        )
        lf = lf.filter(pl.col(IndCQC.estimate_filled_posts_by_job_role) != 0.0)
        diagnostics.checkpoint("after_zero_estimate_filter", lf)

        lf = eUtils.calc_difference_between_estimate_filled_posts_and_summed_job_roles(
            lf
        )
        diagnostics.checkpoint("after_calc_difference", lf)

        lf = add_job_role_groups_column(lf, IndCQC.main_job_group_labelled)

        lf = lf.with_columns(
            pl.col(IndCQC.cqc_location_import_date).dt.year().alias(Keys.year)
        )
        diagnostics.checkpoint("before_sink", lf)

        utils.sink_to_parquet(
            lazy_df=lf,
            output_path=estimated_data_destination,
            partition_cols=[Keys.year],
        )
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


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
