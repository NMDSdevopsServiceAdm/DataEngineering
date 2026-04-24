import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

JobRoleEnumType = pl.Enum(AscwdsWorkerValueLabelsJobGroup.all_roles())


def join_estimates_to_ascwds(
    estimates_lf: pl.LazyFrame,
    ascwds_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join job role estimates to ASCWDS counts ensuring a row for every job role.

    Performs a cross join on the estimates join keys first to ensure there is a row for
    every job role across all time periods for each location. This is then joined with
    the ASCWDS data.

    Args:
        estimates_lf (pl.LazyFrame): Input LazyFrame with Estimates data.
        ascwds_lf (pl.LazyFrame): Input LazyFrame with ASC-WDS job role counts.

    Returns:
        pl.LazyFrame: Joined LazyFrame with a row for every job role.

    """
    join_keys = [
        IndCQC.ascwds_workplace_import_date,
        IndCQC.establishment_id,
    ]
    job_role_labels = IndCQC.main_job_role_clean_labelled

    narrow_keys_lf = estimates_lf.select(
        [IndCQC.id_per_locationid_import_date] + join_keys
    )

    roles_lf = pl.LazyFrame(
        data=[(role,) for role in AscwdsWorkerValueLabelsJobGroup.all_roles()],
        schema={job_role_labels: JobRoleEnumType},
        orient="row",
    )

    expanded_keys_lf = narrow_keys_lf.join(roles_lf, how="cross")

    expanded_counts_lf = expanded_keys_lf.join(
        other=ascwds_lf,
        on=join_keys + [job_role_labels],
        how="left",
    )

    return estimates_lf.join(
        expanded_counts_lf.drop(join_keys),
        on=IndCQC.id_per_locationid_import_date,
        how="right",
    ).drop(join_keys)
