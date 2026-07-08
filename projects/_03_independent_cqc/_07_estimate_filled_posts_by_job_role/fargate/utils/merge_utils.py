from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)

job_role_labels = IndCQC.main_job_role_clean_labelled
# Cache the stable, ordered list of job role labels for reuse and determinism.
MAIN_JOB_ROLE_VALUES = CatVals.main_job_role_labels_column_values.categorical_values


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

    roles_lf = create_job_role_lazyframe()

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


def create_job_role_lazyframe() -> pl.LazyFrame:
    """
    Creates a LazyFrame with one column containing a row per job role label.

    Returns:
        pl.LazyFrame: A LazyFrame of job role labels.
    """
    values = MAIN_JOB_ROLE_VALUES
    # Build explicit 1-tuple rows so Polars interprets each string as a single column value.
    rows = [(v,) for v in values]
    return pl.LazyFrame(
        data=rows,
        schema={job_role_labels: pl.Enum(values)},
        orient="row",
    )
