import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# THROWAWAY: ticket 2110 N=20 stress-test scaffolding, ported from ticket 2000's
# stress_test_utils.py (tag spike-2000-l2-n20). See ticket_2110_zero_jr.md.
# Strip before any real-build promotion.

DUMMY_VALUE_RESOLUTION = 10_000


def add_dummy_employment_status_columns(
    lf: pl.LazyFrame,
    dummy_count: int,
    null_pattern_source_column: str,
) -> tuple[pl.LazyFrame, dict[str, str]]:
    """
    Adds synthetic employment status percentage columns for stress testing.

    Entirely lazy: each value is a hash of (location_id, import date, label)
    scaled to [0, 1), so nothing random is materialised Python-side at real
    data volume.

    Every dummy column copies null_pattern_source_column's null mask exactly,
    matching the real statuses' all-or-nothing-per-row nulls. This matters: the
    L1 normalise step only normalises imputed rows, so independent nulls would
    expose that known bug and turn a compute-cost test into a correctness one.

    Args:
        lf (pl.LazyFrame): Employment status data to augment.
        dummy_count (int): Number of dummy status columns to add.
        null_pattern_source_column (str): Existing percentage column whose null
            pattern is copied onto every dummy column.

    Returns:
        tuple[pl.LazyFrame, dict[str, str]]: The augmented LazyFrame, and a
            mapping of each dummy label to its new percentage column, ready to
            merge into the employment status percentage column mapping.
    """
    is_null = pl.col(null_pattern_source_column).is_null()

    dummy_columns: dict[str, str] = {}
    exprs = []
    for i in range(1, dummy_count + 1):
        label = f"dummy_status_{i:02d}"
        column = f"{label}_percentage"
        dummy_columns[label] = column

        row_hash = pl.concat_str(
            [
                pl.col(IndCQC.location_id).cast(pl.String),
                pl.col(IndCQC.cqc_location_import_date).cast(pl.String),
                pl.lit(label),
            ]
        ).hash()
        value = (row_hash % DUMMY_VALUE_RESOLUTION).cast(
            pl.Float32
        ) / DUMMY_VALUE_RESOLUTION

        exprs.append(pl.when(is_null).then(None).otherwise(value).alias(column))

    return lf.with_columns(exprs), dummy_columns
