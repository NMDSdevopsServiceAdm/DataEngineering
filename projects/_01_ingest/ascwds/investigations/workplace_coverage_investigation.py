# %% [markdown]
# # ASC-WDS workplace coverage investigation
#
# Ad hoc investigation, not part of any pipeline. Answers: how much SLV
# (starters/leavers/vacancies + employees, by job role) coverage do we have,
# and what does that imply about bias in who reports it? Also looks at
# workplace-level reporting behaviour (how often, how regularly), and
# compares "original" (not deduplicated) against "deduped" data throughout
# so the effect of dedup itself is visible rather than assumed.
#
# Copy this whole file into a SageMaker notebook and run cell by cell (the
# `# %%` markers are recognised as cell boundaries by both VS Code/Jupytext
# and SageMaker's Jupyter). Only cell 1 (CONFIG) should need editing.
#
# The functions in the "generic coverage toolkit" section take a LazyFrame,
# column name(s) and optional group-by column(s) - nothing ASC-WDS-specific.
# Reuse them as-is for other variables (e.g. worker gender, pay) or other
# datasets (e.g. re-running post-filtering to see how coverage/bias changed).
# The "apply to ASC-WDS workplace SLV data" section below is the only part
# wired to this specific dataset.

# %%
from dataclasses import asdict
from pathlib import Path

import polars as pl

import polars_utils.expressions as expr
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_values.categorical_column_values import MainJobRoleID

# %% [markdown]
# ## Config - edit these for your run

# %%
# Cleaned ASC-WDS workplace output from clean_ascwds_workplace.py (already
# deduplicated to one row per location_id per ascwds_workplace_import_date,
# with SLV job-role columns bounded).
CLEANED_WORKPLACE_SOURCE = "s3://sfc-.../cleaned_ascwds_workplace/"  # <- edit me

# An undeduplicated equivalent (same cleaning/bounding/label steps as
# CLEANED_WORKPLACE_SOURCE, just without remove_rows_with_duplicate_location_ids
# applied), so results can be compared with vs without dedup.
ORIGINAL_WORKPLACE_SOURCE = "s3://sfc-.../original_ascwds_workplace/"  # <- edit me

# Each result table is written here (as csv, small/human-readable summaries)
# as well as printed, so nothing is lost if the notebook kernel restarts.
OUTPUT_DIR = Path("coverage_investigation_outputs")  # <- edit me
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Bins for grouping workplace size. Edit to taste - number of labels must
# always be len(breaks) + 1.
WORKPLACE_SIZE_BAND_BREAKS = [2, 5, 10, 25, 50, 100]
WORKPLACE_SIZE_BAND_LABELS = ["1-2", "3-5", "6-10", "11-25", "26-50", "51-100", "100+"]

# Bins for grouping the gap between two consecutive genuine updates.
UPDATE_GAP_BREAKS_DAYS = [30, 90, 180, 365, 730]
UPDATE_GAP_LABELS = ["<=30d", "31-90d", "91-180d", "181-365d", "1-2yr", "2yr+"]

# ASC-WDS has renumbered some job roles over time; clean_ascwds_workplace.py
# doesn't yet fold historic codes into their current equivalent, so it's
# done below instead. {current_role: [historic_role, ...]} - current_role is
# the full column prefix (e.g. "jr27"), historic_role is just the numeric id
# (e.g. "22", matching jr22emp/jr22strt/jr22stop/jr22vacy).
HISTORIC_JOB_ROLE_MAPPING = {
    "jr27": ["22"],
    "jr40": ["41"],
    "jr42": ["12", "13", "14", "18", "19", "20", "21"],
}
# Must match the suffixes in polars_utils.expressions.is_slv_job_role_column().
SLV_SUFFIXES = ["emp", "strt", "stop", "vacy"]


# %% [markdown]
# ## Generic coverage toolkit
#
# Everything here is parameterised by column name(s) - no ASC-WDS-specific
# logic - so it works unchanged on other variables or other LazyFrames.


# %%
def save_result(df: pl.DataFrame, name: str) -> pl.DataFrame:
    """Print a result and persist it to OUTPUT_DIR/{name}.csv.

    Args:
        df (pl.DataFrame): result table to persist.
        name (str): file name (without extension) to save under.

    Returns:
        pl.DataFrame: the same df, unchanged, for continued use in-notebook.
    """
    output_path = OUTPUT_DIR / f"{name}.csv"
    df.write_csv(output_path)
    print(f"--- {name} ({df.height} rows) -> {output_path} ---")
    print(df)
    return df


def run_for_each_dataset(
    datasets: dict[str, pl.LazyFrame],
    analysis_fn,
) -> pl.DataFrame:
    """Run the same analysis over several named datasets and stack the results.

    Use this to compare variants of the same data (e.g. original vs
    deduplicated, or before/after a filtering step) - every other function
    here operates on a single LazyFrame, so this just repeats that call per
    dataset and tags each result with which one produced it.

    Args:
        datasets (dict[str, pl.LazyFrame]): {dataset_name: lf}, e.g.
            {"original": ..., "deduped": ...}.
        analysis_fn (Callable[[pl.LazyFrame], pl.DataFrame]): analysis to run
            on each dataset, e.g. `lambda lf: coverage_by_group(lf, cols)`.

    Returns:
        pl.DataFrame: the concatenation of each dataset's result, with a
            "dataset" column identifying which one produced each row.
    """
    return pl.concat(
        analysis_fn(dataset_lf).with_columns(pl.lit(dataset_name).alias("dataset"))
        for dataset_name, dataset_lf in datasets.items()
    )


def coverage_by_group(
    lf: pl.LazyFrame,
    variables: list[str],
    group_by: list[str] | None = None,
) -> pl.DataFrame:
    """Coverage (non-null rate) of one or more variables, optionally by group.

    Args:
        lf (pl.LazyFrame): source data, one row per observation.
        variables (list[str]): columns to measure coverage for.
        group_by (list[str] | None): columns to group by (e.g. region, main
            service). Pass None (or []) for overall coverage with no grouping.

    Returns:
        pl.DataFrame: long format, one row per (group, variable) with
            n_non_null, n_total and coverage_pct.
    """
    group_by = group_by or []

    non_null_exprs = [pl.col(var).is_not_null().sum().alias(var) for var in variables]
    counts_lf = (
        lf.group_by(group_by).agg(*non_null_exprs, pl.len().alias("n_total"))
        if group_by
        else lf.select(*non_null_exprs, pl.len().alias("n_total"))
    )

    coverage_df = (
        counts_lf.collect()
        .unpivot(
            on=variables,
            index=[*group_by, "n_total"],
            variable_name="variable",
            value_name="n_non_null",
        )
        .with_columns(
            (pl.col("n_non_null") / pl.col("n_total")).alias("coverage_pct")
        )
        .sort([*group_by, "variable"])
    )
    return coverage_df


def sum_by_group(
    lf: pl.LazyFrame,
    variables: list[str],
    group_by: list[str] | None = None,
) -> pl.DataFrame:
    """Total and mean of one or more variables' actual values, optionally by group.

    Unlike coverage_by_group() (which only measures whether a value is
    present), this measures the values themselves - e.g. the actual number
    of employees/starters/leavers/vacancies reported, not just whether the
    column was filled in.

    Args:
        lf (pl.LazyFrame): source data, one row per observation.
        variables (list[str]): numeric columns to total.
        group_by (list[str] | None): columns to group by. Pass None (or [])
            for an overall total with no grouping.

    Returns:
        pl.DataFrame: long format, one row per (group, variable) with
            n_non_null (how many rows contributed a value), value_sum and
            mean_value (value_sum / n_non_null).
    """
    group_by = group_by or []

    sum_exprs = [pl.col(var).sum().alias(var) for var in variables]
    non_null_exprs = [
        pl.col(var).is_not_null().sum().alias(f"{var}__n_non_null") for var in variables
    ]
    agg_lf = (
        lf.group_by(group_by).agg(*sum_exprs, *non_null_exprs)
        if group_by
        else lf.select(*sum_exprs, *non_null_exprs)
    )
    agg_df = agg_lf.collect()

    sums_df = agg_df.unpivot(
        on=variables, index=group_by, variable_name="variable", value_name="value_sum"
    )
    counts_df = agg_df.unpivot(
        on=[f"{var}__n_non_null" for var in variables],
        index=group_by,
        variable_name="variable",
        value_name="n_non_null",
    ).with_columns(pl.col("variable").str.strip_suffix("__n_non_null"))

    return (
        sums_df.join(counts_df, on=[*group_by, "variable"])
        .with_columns(
            pl.when(pl.col("n_non_null") > 0)
            .then(pl.col("value_sum") / pl.col("n_non_null"))
            .otherwise(None)
            .alias("mean_value")
        )
        .sort([*group_by, "variable"])
    )


def summarise_across_variables(
    coverage_df: pl.DataFrame,
    group_by: list[str] | None = None,
) -> pl.DataFrame:
    """Collapse a coverage_by_group() output to a single mean_coverage_pct per group.

    Useful when several variables (e.g. all SLV job-role columns) are being
    treated as one headline measure rather than reported individually.

    Args:
        coverage_df (pl.DataFrame): output of coverage_by_group().
        group_by (list[str] | None): grouping columns present in coverage_df
            (must match what was passed into coverage_by_group()).

    Returns:
        pl.DataFrame: one row per group with mean_coverage_pct.
    """
    group_by = group_by or []
    if group_by:
        return (
            coverage_df.group_by(group_by)
            .agg(pl.col("coverage_pct").mean().alias("mean_coverage_pct"))
            .sort(group_by)
        )
    return coverage_df.select(pl.col("coverage_pct").mean().alias("mean_coverage_pct"))


def add_band(
    lf: pl.LazyFrame,
    column: str,
    breaks: list[float],
    labels: list[str],
    band_column: str | None = None,
) -> pl.LazyFrame:
    """Bucket a numeric column into bands (e.g. workplace size, pay).

    Args:
        lf (pl.LazyFrame): source data.
        column (str): numeric column to band.
        breaks (list[float]): cut points.
        labels (list[str]): band labels, must be len(breaks) + 1.
        band_column (str | None): name for the new column. Defaults to
            "{column}_band".

    Returns:
        pl.LazyFrame: input frame with the band column added.
    """
    band_column = band_column or f"{column}_band"
    return lf.with_columns(
        pl.col(column).cut(breaks, labels=labels).alias(band_column)
    )


def workplace_update_counts(
    lf: pl.LazyFrame,
    id_column: str,
    update_date_column: str,
) -> pl.DataFrame:
    """Per-workplace count of distinct update dates and the span they cover.

    Args:
        lf (pl.LazyFrame): source data, one row per observation.
        id_column (str): column identifying a workplace over time.
        update_date_column (str): date column marking when the workplace's
            data last changed (use a genuine "last amended" date, not the
            snapshot/import date, or every recurring workplace will look
            like it "updates" every period regardless of whether anything
            changed).

    Returns:
        pl.DataFrame: one row per workplace with n_distinct_updates,
            first_update_date, last_update_date.
    """
    return (
        lf.group_by(id_column)
        .agg(
            pl.col(update_date_column).n_unique().alias("n_distinct_updates"),
            pl.col(update_date_column).min().alias("first_update_date"),
            pl.col(update_date_column).max().alias("last_update_date"),
        )
        .collect()
    )


def value_distribution(
    df: pl.DataFrame,
    value_column: str,
    group_by: list[str] | None = None,
) -> pl.DataFrame:
    """Distribution (count and %) of a discrete column's values.

    Args:
        df (pl.DataFrame): source data, one row per observation.
        value_column (str): column to distribute over (e.g.
            n_distinct_updates, or a pre-computed band column).
        group_by (list[str] | None): columns to compute the distribution
            separately within (e.g. "dataset", so original vs deduped don't
            get mixed into one distribution). Pass None (or []) for a single
            distribution across all rows.

    Returns:
        pl.DataFrame: one row per (group, distinct value) with n and pct,
            where pct is relative to its own group's total.
    """
    group_by = group_by or []
    counts_df = df.group_by([*group_by, value_column]).agg(pl.len().alias("n"))

    if group_by:
        totals_df = df.group_by(group_by).agg(pl.len().alias("_group_total"))
        return (
            counts_df.join(totals_df, on=group_by)
            .with_columns((pl.col("n") / pl.col("_group_total")).alias("pct"))
            .drop("_group_total")
            .sort([*group_by, value_column])
        )

    total = df.height
    return counts_df.with_columns((pl.col("n") / total).alias("pct")).sort(value_column)


def merge_historic_job_roles(
    lf: pl.LazyFrame,
    job_role_mapping: dict[str, list[str]],
    suffixes: list[str],
) -> pl.LazyFrame:
    """Fold historic (retired) job role codes into their current equivalent.

    For each current_role: historic_roles pair and each suffix, adds the
    historic role's column(s) onto the current role's column of the same
    suffix (e.g. jr22emp is added into jr27emp), then drops the historic
    columns. A row where every contributing column is null is kept null
    rather than turned into a false 0.

    Args:
        lf (pl.LazyFrame): source data containing jr<role><suffix> columns.
        job_role_mapping (dict[str, list[str]]): {current_role: [historic_role, ...]},
            where current_role is the full column prefix (e.g. "jr27") and
            historic_role is just the numeric id (e.g. "22").
        suffixes (list[str]): SLV metric suffixes to merge (e.g. "emp").

    Returns:
        pl.LazyFrame: input frame with historic role columns folded into
            their current equivalents and dropped. Columns named in
            job_role_mapping that aren't present in lf are skipped.
    """
    schema_columns = set(lf.collect_schema().names())

    merge_exprs = []
    columns_to_drop = []
    for current_role, historic_roles in job_role_mapping.items():
        for suffix in suffixes:
            current_column = f"{current_role}{suffix}"
            historic_columns = [
                f"jr{historic_role}{suffix}"
                for historic_role in historic_roles
                if f"jr{historic_role}{suffix}" in schema_columns
            ]
            if current_column not in schema_columns or not historic_columns:
                continue

            columns_to_merge = [current_column, *historic_columns]
            merge_exprs.append(
                pl.when(pl.all_horizontal(pl.col(columns_to_merge).is_null()))
                .then(None)
                .otherwise(pl.sum_horizontal(pl.col(columns_to_merge)))
                .alias(current_column)
            )
            columns_to_drop.extend(historic_columns)

    return lf.with_columns(merge_exprs).drop(columns_to_drop)


def time_between_updates(
    lf: pl.LazyFrame,
    id_column: str,
    update_date_column: str,
) -> pl.DataFrame:
    """Days between each workplace's consecutive genuine updates.

    Args:
        lf (pl.LazyFrame): source data, one row per observation.
        id_column (str): column identifying a workplace over time.
        update_date_column (str): date column marking when the workplace's
            data last changed (see workplace_update_counts() docstring on why
            this shouldn't be the snapshot/import date).

    Returns:
        pl.DataFrame: one row per (workplace, update) gap, with
            days_since_previous_update. A workplace's first observed update
            has no prior gap and is excluded.
    """
    distinct_updates_lf = (
        lf.select(id_column, update_date_column).unique().sort([id_column, update_date_column])
    )
    gaps_lf = distinct_updates_lf.with_columns(
        (
            pl.col(update_date_column)
            - pl.col(update_date_column).shift(1).over(id_column)
        )
        .dt.total_days()
        .alias("days_since_previous_update")
    ).filter(pl.col("days_since_previous_update").is_not_null())
    return gaps_lf.collect()


# %% [markdown]
# ## Apply to ASC-WDS workplace SLV data
#
# Every analysis below runs on both "original" (not deduplicated) and
# "deduped" data and tags results with a `dataset` column, so the effect of
# `remove_rows_with_duplicate_location_ids()` on coverage/bias can be seen
# directly rather than assumed. ORIGINAL_WORKPLACE_SOURCE must have been
# through the same cleaning/bounding/label steps as CLEANED_WORKPLACE_SOURCE
# - the only intended difference is that dedup step.

# %%
# Do the historic job role merge before anything else: otherwise those
# codes would be analysed as if they were separate/missing roles rather than
# folded into the current role they actually represent.
WORKPLACE_LAZYFRAMES = {
    "original": merge_historic_job_roles(
        pl.scan_parquet(ORIGINAL_WORKPLACE_SOURCE), HISTORIC_JOB_ROLE_MAPPING, SLV_SUFFIXES
    ),
    "deduped": merge_historic_job_roles(
        pl.scan_parquet(CLEANED_WORKPLACE_SOURCE), HISTORIC_JOB_ROLE_MAPPING, SLV_SUFFIXES
    ),
}

SLV_COLUMNS = (
    WORKPLACE_LAZYFRAMES["deduped"].select(expr.is_slv_job_role_column()).collect_schema().names()
)
HEADLINE_COLUMNS = [AWPClean.total_staff_bounded, AWPClean.worker_records_bounded, *SLV_COLUMNS]

# MainJobRoleID is a ColumnValues dataclass (column_name is just descriptive
# metadata, unused here) mapping role name -> id; drop the non-role metadata
# fields and reverse it to id -> name for joining onto job_role_id below.
_job_role_id_by_name = asdict(MainJobRoleID(column_name="main_job_role_id"))
for _metadata_field in ("column_name", "value_to_remove", "contains_null_values"):
    _job_role_id_by_name.pop(_metadata_field)
JOB_ROLE_NAME_BY_ID = {role_id: name for name, role_id in _job_role_id_by_name.items()}


def add_job_role_id(coverage_df: pl.DataFrame) -> pl.DataFrame:
    """Attach job_role_id/job_role_name to a coverage_by_group() output over SLV_COLUMNS."""
    return coverage_df.with_columns(
        pl.col("variable").str.extract(r"jr(\d+)", 1).str.strip_chars_start("0").alias("job_role_id")
    ).with_columns(pl.col("job_role_id").replace(JOB_ROLE_NAME_BY_ID).alias("job_role_name"))


# %% [markdown]
# ### Overall coverage

# %%
overall_coverage_df = save_result(
    run_for_each_dataset(WORKPLACE_LAZYFRAMES, lambda lf: coverage_by_group(lf, HEADLINE_COLUMNS)),
    "overall_coverage",
)
save_result(
    summarise_across_variables(overall_coverage_df, group_by=["dataset"]),
    "overall_coverage_summary",
)

# %% [markdown]
# ### By job role level
#
# Each SLV column is `jr<role_id>emp/strt/stop/vacy`; average the four
# metrics per role to get one coverage figure per job role, and attach the
# human-readable role name from MainJobRoleID.

# %%
job_role_coverage_df = save_result(
    add_job_role_id(
        run_for_each_dataset(WORKPLACE_LAZYFRAMES, lambda lf: coverage_by_group(lf, SLV_COLUMNS))
    )
    .group_by(["job_role_id", "job_role_name", "dataset"])
    .agg(pl.col("coverage_pct").mean().alias("mean_coverage_pct"))
    .sort(["dataset", "mean_coverage_pct"]),
    "job_role_coverage",
)

# %% [markdown]
# ### By region
#
# NB: `cssr` isn't in the cleaned workplace schema (clean_ascwds_workplace.py
# doesn't currently select it from the raw data), so region_id is the finest
# geography available here. To break down by cssr as well, add
# `AWP.cssr` to `WORKPLACE_SCHEMA` in clean_ascwds_workplace.py first.

# %%
region_coverage_df = save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: coverage_by_group(lf, SLV_COLUMNS, group_by=[AWPClean.region_id]),
    ),
    "region_coverage_detailed",
)
save_result(
    summarise_across_variables(region_coverage_df, group_by=[AWPClean.region_id, "dataset"]),
    "region_coverage_summary",
)

# %% [markdown]
# ### By main service

# %%
main_service_coverage_df = save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: coverage_by_group(lf, SLV_COLUMNS, group_by=[AWPClean.main_service_id]),
    ),
    "main_service_coverage_detailed",
)
save_result(
    summarise_across_variables(main_service_coverage_df, group_by=[AWPClean.main_service_id, "dataset"]),
    "main_service_coverage_summary",
)

# %% [markdown]
# ### By main service and workplace size

# %%
WORKPLACE_LAZYFRAMES_WITH_SIZE_BAND = {
    dataset_name: add_band(
        dataset_lf,
        AWPClean.total_staff_bounded,
        WORKPLACE_SIZE_BAND_BREAKS,
        WORKPLACE_SIZE_BAND_LABELS,
        band_column="workplace_size_band",
    )
    for dataset_name, dataset_lf in WORKPLACE_LAZYFRAMES.items()
}

main_service_and_size_coverage_df = save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES_WITH_SIZE_BAND,
        lambda lf: coverage_by_group(
            lf, SLV_COLUMNS, group_by=[AWPClean.main_service_id, "workplace_size_band"]
        ),
    ),
    "main_service_and_size_coverage_detailed",
)
save_result(
    summarise_across_variables(
        main_service_and_size_coverage_df,
        group_by=[AWPClean.main_service_id, "workplace_size_band", "dataset"],
    ),
    "main_service_and_size_coverage_summary",
)

# %% [markdown]
# ### Trends over time of SLV coverage, by breakdown
#
# Bias isn't necessarily static - coverage may have improved/worsened over
# time, and not evenly across regions/roles/services. The no-split and
# by-job-role trends below both reuse the same per-column-per-period detailed
# table computed first, since job_role_id/name is derived from its "variable"
# column; region and main service need to be part of the grouping up front,
# so those two recompute from WORKPLACE_LAZYFRAMES directly.

# %%
coverage_trend_detailed_df = save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: coverage_by_group(lf, SLV_COLUMNS, group_by=[AWPClean.ascwds_workplace_import_date]),
    ),
    "coverage_trend_detailed",
)

# %% [markdown]
# #### No splits

# %%
save_result(
    summarise_across_variables(
        coverage_trend_detailed_df, group_by=[AWPClean.ascwds_workplace_import_date, "dataset"]
    ),
    "coverage_trend_no_split",
)

# %% [markdown]
# #### By region

# %%
coverage_trend_by_region_df = run_for_each_dataset(
    WORKPLACE_LAZYFRAMES,
    lambda lf: coverage_by_group(
        lf, SLV_COLUMNS, group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.region_id]
    ),
)
save_result(
    summarise_across_variables(
        coverage_trend_by_region_df,
        group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.region_id, "dataset"],
    ),
    "coverage_trend_by_region",
)

# %% [markdown]
# #### By job role

# %%
save_result(
    add_job_role_id(coverage_trend_detailed_df)
    .group_by([AWPClean.ascwds_workplace_import_date, "job_role_id", "job_role_name", "dataset"])
    .agg(pl.col("coverage_pct").mean().alias("mean_coverage_pct"))
    .sort([AWPClean.ascwds_workplace_import_date, "dataset", "job_role_id"]),
    "coverage_trend_by_job_role",
)

# %% [markdown]
# #### By main service

# %%
coverage_trend_by_main_service_df = run_for_each_dataset(
    WORKPLACE_LAZYFRAMES,
    lambda lf: coverage_by_group(
        lf, SLV_COLUMNS, group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.main_service_id]
    ),
)
save_result(
    summarise_across_variables(
        coverage_trend_by_main_service_df,
        group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.main_service_id, "dataset"],
    ),
    "coverage_trend_by_main_service",
)

# %% [markdown]
# ### Trends over time of actual SLV data, by breakdown
#
# Coverage only tells you whether a value was reported, not what it was.
# These trend the actual reported numbers instead. total_staff_bounded and
# worker_records_bounded (HEADLINE_TOTAL_COLUMNS) are used for the
# no-split/region/main-service views, since summing SLV metrics of
# different kinds (employees, starters, leavers, vacancies) together
# wouldn't mean anything; the job role breakdown uses SLV_COLUMNS directly
# and keeps each metric separate for the same reason.

# %%
HEADLINE_TOTAL_COLUMNS = [AWPClean.total_staff_bounded, AWPClean.worker_records_bounded]

# %% [markdown]
# #### No splits

# %%
save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: sum_by_group(
            lf, HEADLINE_TOTAL_COLUMNS, group_by=[AWPClean.ascwds_workplace_import_date]
        ),
    ),
    "value_trend_no_split",
)

# %% [markdown]
# #### By region

# %%
save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: sum_by_group(
            lf,
            HEADLINE_TOTAL_COLUMNS,
            group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.region_id],
        ),
    ),
    "value_trend_by_region",
)

# %% [markdown]
# #### By job role

# %%
save_result(
    add_job_role_id(
        run_for_each_dataset(
            WORKPLACE_LAZYFRAMES,
            lambda lf: sum_by_group(lf, SLV_COLUMNS, group_by=[AWPClean.ascwds_workplace_import_date]),
        )
    ).sort([AWPClean.ascwds_workplace_import_date, "dataset", "job_role_id"]),
    "value_trend_by_job_role",
)

# %% [markdown]
# #### By main service

# %%
save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: sum_by_group(
            lf,
            HEADLINE_TOTAL_COLUMNS,
            group_by=[AWPClean.ascwds_workplace_import_date, AWPClean.main_service_id],
        ),
    ),
    "value_trend_by_main_service",
)

# %% [markdown]
# ### Anything else? Parent vs single-site

# %%
coverage_by_parent_status_df = save_result(
    run_for_each_dataset(
        WORKPLACE_LAZYFRAMES,
        lambda lf: coverage_by_group(lf, SLV_COLUMNS, group_by=[AWPClean.is_parent]),
    ),
    "coverage_by_parent_status_detailed",
)
save_result(
    summarise_across_variables(coverage_by_parent_status_df, group_by=[AWPClean.is_parent, "dataset"]),
    "coverage_by_parent_status_summary",
)

# %% [markdown]
# ## Workplace-level analysis
#
# Uses `establishment_id` as the identifier for "the same workplace over
# time" and `data_last_amended_date` (not the monthly snapshot/import date)
# as the update event, so that a workplace which simply reappears in every
# monthly extract without changing its data isn't counted as "updating"
# every month.

# %% [markdown]
# ### Distribution of how many times workplaces update

# %%
update_counts_df = run_for_each_dataset(
    WORKPLACE_LAZYFRAMES,
    lambda lf: workplace_update_counts(lf, AWPClean.establishment_id, AWPClean.data_last_amended_date),
)
save_result(
    value_distribution(update_counts_df, "n_distinct_updates", group_by=["dataset"]),
    "update_count_distribution",
)

# %% [markdown]
# ### Distribution of length of time between updates

# %%
gaps_df = run_for_each_dataset(
    WORKPLACE_LAZYFRAMES,
    lambda lf: time_between_updates(lf, AWPClean.establishment_id, AWPClean.data_last_amended_date),
)
gaps_banded_df = add_band(
    gaps_df.lazy(),
    "days_since_previous_update",
    UPDATE_GAP_BREAKS_DAYS,
    UPDATE_GAP_LABELS,
    band_column="gap_band",
).collect()
save_result(
    value_distribution(gaps_banded_df, "gap_band", group_by=["dataset"]),
    "time_between_updates_distribution",
)
save_result(
    gaps_df.group_by("dataset").agg(
        pl.col("days_since_previous_update").mean().alias("mean_days"),
        pl.col("days_since_previous_update").median().alias("median_days"),
        pl.col("days_since_previous_update").min().alias("min_days"),
        pl.col("days_since_previous_update").max().alias("max_days"),
    ),
    "time_between_updates_describe",
)

# %% [markdown]
# ## Reuse on other variables/datasets
#
# The toolkit functions don't know anything about ASC-WDS. For example, to
# check coverage of worker-level gender and pay after cleaning:
#
# ```python
# worker_lf = pl.scan_parquet("s3://.../cleaned_ascwds_worker/")
# coverage_by_group(worker_lf, [AWKClean.gender, AWKClean.pay_hourly_rate])
# coverage_by_group(
#     worker_lf, [AWKClean.gender, AWKClean.pay_hourly_rate], group_by=[AWKClean.main_job_role]
# )
# sum_by_group(worker_lf, [AWKClean.pay_hourly_rate], group_by=[AWKClean.main_job_role])
# ```
#
# Or to see how a downstream filtering step changes coverage, just point
# `coverage_by_group()` at the filtered LazyFrame instead and compare.
