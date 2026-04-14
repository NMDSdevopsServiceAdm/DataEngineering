import logging
import os
import tempfile
from pathlib import Path
from typing import Final

import polars as pl
import polars.selectors as cs

from polars_utils import utils
from polars_utils.pipeline_utils import log_polars_plan, time_it
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    ManagerialFilledPostAdjustmentExpr,
    nullify_job_role_count_when_source_not_ascwds,
    percentage_share,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

logger = logging.getLogger(__name__)

polars_temp_dir = os.getenv("POLARS_TEMP_DIR", tempfile.gettempdir())
logging.info(f"Polars temp dir set at: {polars_temp_dir}")
CHECKPOINT_PATH = Path(polars_temp_dir) / "checkpoints"

# Define constants for IDs for original length data and expanded data.
ROW_ID: Final[str] = "id"
EXPANDED_ID: Final[str] = "expanded_id"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

EstablishmentCatType = pl.Categorical(
    pl.Categories("establishment", namespace="filled_posts")
)
LocationCatType = pl.Categorical(pl.Categories("location", namespace="filled_posts"))
JobRoleEnumType = pl.Enum(AscwdsWorkerValueLabelsJobGroup.all_roles())
EstimatesFilledPostSourceEnumType = pl.Enum(
    [
        "imputed_pir_filled_posts_model",
        "ascwds_pir_merged",
        "imputed_posts_care_home_model",
        "care_home_model",
        "imputed_posts_non_res_combined_model",
        "non_res_combined_model",
        "posts_rolling_average_model",
    ]
)
PrimaryServiceEnumType = pl.Enum(
    [
        PrimaryServiceType.care_home_only,
        PrimaryServiceType.care_home_with_nursing,
        PrimaryServiceType.non_residential,
    ]
)

metadata_columns = {
    IndCQC.name: str,
    IndCQC.provider_id: str,
    IndCQC.services_offered: pl.List(str),
    IndCQC.primary_service_type: PrimaryServiceEnumType,
    IndCQC.primary_service_type_second_level: pl.Categorical,
    IndCQC.care_home: pl.Categorical,
    IndCQC.dormancy: pl.Categorical,
    IndCQC.number_of_beds: pl.Int16,
    IndCQC.imputed_registration_date: pl.Date,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.organisation_id: str,
    IndCQC.worker_records_bounded: pl.Int16,
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
    IndCQC.ascwds_pir_merged: pl.Float32,
    IndCQC.ascwds_filtering_rule: pl.Categorical,
    IndCQC.current_ons_import_date: pl.Date,
    IndCQC.current_cssr: pl.Categorical,
    IndCQC.current_region: pl.Categorical,
    IndCQC.current_icb: pl.Categorical,
    IndCQC.current_rural_urban_indicator_2011: pl.Categorical,
    IndCQC.current_lsoa21: pl.Categorical,
    IndCQC.current_msoa21: pl.Categorical,
    IndCQC.estimate_filled_posts_source: EstimatesFilledPostSourceEnumType,
}
ascwds_columns_to_import = {
    IndCQC.ascwds_worker_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.main_job_role_clean_labelled: JobRoleEnumType,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}
transformation_columns = {
    IndCQC.location_id: LocationCatType,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.estimate_filled_posts_source: EstimatesFilledPostSourceEnumType,
    IndCQC.primary_service_type: PrimaryServiceEnumType,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
}


def main(
    estimates_source: str,
    ascwds_job_role_counts_source: str,
    estimates_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        estimates_by_job_role_destination (str): destination for output
    """

    # Shall we split this script into separate scripts and create a new paralel state in the estimates pipeline,
    # in paralel with validate estimate filled posts.
    # So we can work on different parts of the script without using LiveShare.

    # Remove time_it.
    with time_it("scan_and_join"):

        # Read in estimates dataset, split into cols to transform and everything else, then
        # read in aggreagated worker data and join to estimates (the selected cols version).

        combined_schema = transformation_columns | metadata_columns
        full_estimates_lf = (
            pl.scan_parquet(estimates_source, low_memory=True)
            .select(list(combined_schema))
            # Add row id index for single-key joining.
            .with_row_index(name=ROW_ID)
            .with_columns(utils.cast_to_schema(combined_schema))
        )
        estimated_posts_base_lf = full_estimates_lf.select(
            ROW_ID, *list(transformation_columns)
        )
        # This will be joined on at the end.
        metadata_lf = full_estimates_lf.select(ROW_ID, *list(metadata_columns))

        col_name_map = {
            IndCQC.ascwds_worker_import_date: IndCQC.ascwds_workplace_import_date
        }
        ascwds_job_role_counts_lf = (
            pl.scan_parquet(ascwds_job_role_counts_source, low_memory=True)
            .select(list(ascwds_columns_to_import))
            .with_columns(utils.cast_to_schema(ascwds_columns_to_import))
            # Rename to avoid providing left + right "on" in subsequent join.
            .rename(col_name_map)
        )

        estimated_job_role_posts_lf = join_estimates_to_ascwds(
            estimated_posts_base_lf,
            ascwds_job_role_counts_lf,
        )

        # Start of cleaning jobs.
        # Null job role counts when we've not used ASC-WDS for estimated posts.

        estimated_job_role_posts_lf = nullify_job_role_count_when_source_not_ascwds(
            estimated_job_role_posts_lf
        ).drop(
            IndCQC.estimate_filled_posts_source,
            IndCQC.ascwds_filled_posts_dedup_clean,
        )

        # TODO - Filter ASC-WDS worker data.

        estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_row_index(
            EXPANDED_ID
        )

        # Remove log_polars_plan.
        log_polars_plan(estimated_job_role_posts_lf, "Post Join")

        # If we're splitting this into separate scripts then we will be saving outputs to S3,
        # so we dont need this part.
        checkpoint_filepath = CHECKPOINT_PATH / "checkpoint1.parquet"
        estimated_job_role_posts_lf.sink_parquet(
            checkpoint_filepath,
            mkdir=True,
            engine="streaming",
        )

    # Start of imputation job.
    # Remove time_it
    with time_it("Impute ratios"):
        estimated_job_role_posts_lf = pl.scan_parquet(
            checkpoint_filepath,
            low_memory=True,
        )

        pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        estimated_job_role_posts_lf = get_percent_share_ratios(
            estimated_job_role_posts_lf,
            input_col=IndCQC.ascwds_job_role_counts,
            output_col=IndCQC.ascwds_job_role_ratios,
        )

        # Rename impute_ratios to be more descriptive. E.g. create_imputed_ascwds_job_role_counts or something similar.
        # Add the multiplication below this function into "impute_ratios".
        estimated_job_role_posts_lf = impute_ratios(estimated_job_role_posts_lf)

        # Multiply imputed ratios by estimate filled posts
        estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
            pl.col(IndCQC.estimate_filled_posts)
            .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
            .alias(IndCQC.imputed_ascwds_job_role_counts)
        )

        # Combine the count rolling sum and get_percent_share_ratio into one function that returns ascwds_job_role_rolling_ratio.
        estimated_job_role_posts_lf = get_job_counts_rolling_sum(
            estimated_job_role_posts_lf
        )
        estimated_job_role_posts_lf = get_percent_share_ratios(
            estimated_job_role_posts_lf,
            input_col="rolling_sum",
            output_col=IndCQC.ascwds_job_role_rolling_ratio,
        )

        # Start of making the estimates by job role using ASC-WDS data only.
        # This will coalesce the job role percentage breakdowns, multiply by filled posts number, then
        # adjust that to account for registered managers from CQC.

        # Abstract this into a defined function. So that main is calling a series of steps.
        estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
            utils.coalesce_with_source_labels(
                cols=[
                    # IndCQC.ascwds_job_role_ratios_filtered,
                    IndCQC.imputed_ascwds_job_role_ratios,
                    IndCQC.ascwds_job_role_rolling_ratio,
                ],
                name=IndCQC.ascwds_job_role_ratios_merged,
            ),
        )

        estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
            (
                pl.col(IndCQC.estimate_filled_posts)
                * pl.col(IndCQC.ascwds_job_role_ratios_merged)
            ).alias(IndCQC.estimate_filled_posts_by_job_role)
        )

        # Remove log_polars_plan.
        log_polars_plan(estimated_job_role_posts_lf, "Impute and rolling")
        checkpoint_filepath = CHECKPOINT_PATH / "checkpoint2.parquet"
        estimated_job_role_posts_lf.sink_parquet(
            checkpoint_filepath,
            mkdir=True,
            engine="streaming",
        )

    # Remove time_it.
    # Try to refactor the pipe(apply_manager_adjustments) so it's readable sequence of expressions without class methods.
    with time_it("Manager adjustments"):
        estimated_job_role_posts_lf = pl.scan_parquet(
            checkpoint_filepath,
            low_memory=True,
        ).pipe(apply_manager_adjustments)

        # Start of preparing columns for validation. Move these to the validation script.

        # Creating the overall estimated filled posts per location from the job role split.
        # We expecting this to be different

        sum_all_job_roles = pl.sum(
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted
        ).alias(IndCQC.estimate_filled_posts_from_all_job_roles)

        # Since this is a scalar reduction per group, no explode is needed.
        final_sum_agg_lf = estimated_job_role_posts_lf.group_by(pct_share_groups).agg(
            sum_all_job_roles
        )

        estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
            final_sum_agg_lf,
            on=pct_share_groups,
            how="left",
        )

        # Move this to before validation preparation starts. So it's the last function in the
        # estimates script.
        # Think about validation failing because DataFrame is too big. Read in estimates by job role
        # as lazyframe, select only columns required for validation, then collect into dataframe.
        # Join back original metadata.
        estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
            metadata_lf,
            on="id",
        )

        # Remove log_polars_plan.
        log_polars_plan(estimated_job_role_posts_lf, "Final Transformation")

        utils.sink_to_parquet(
            lazy_df=estimated_job_role_posts_lf,
            output_path=estimates_by_job_role_destination,
            append=False,
        )


def join_estimates_to_ascwds(
    estimates_lf: pl.LazyFrame,
    ascwds_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join job role estimates to ASCWDS counts ensuring a row for every job role.

    Performs a cross join on the estimates join keys first to ensure there is a row for
    every job role across all time periods for each location. This is then joined with
    the ASCWDS data.
    """
    join_keys = [
        IndCQC.ascwds_workplace_import_date,
        IndCQC.establishment_id,
    ]
    job_role_labels = IndCQC.main_job_role_clean_labelled

    # Narrow select "id" and join keys first to improve memory performance of
    # cross join. From this we get the full amount of rows expected.
    narrow_keys_lf = estimates_lf.select(["id"] + join_keys)
    # This is just a single column df with a row for each job role (~38).
    roles_lf = pl.LazyFrame(
        data=[AscwdsWorkerValueLabelsJobGroup.all_roles()],
        schema={job_role_labels: JobRoleEnumType},
    )
    # This will be the length of estimates dataset x number of job roles.
    expanded_keys_lf = narrow_keys_lf.join(roles_lf, how="cross")

    expanded_counts_lf = expanded_keys_lf.join(
        other=ascwds_lf,
        on=join_keys + [job_role_labels],
        how="left",
    )

    # Re-attach the wide base data - The streaming engine easily handles this
    # 1-to-many join via the 'id' column. Drop the join keys (used earlier) from
    # both sides as they are not relevant to the rest of the pipeline.
    return estimates_lf.join(
        expanded_counts_lf.drop(join_keys),
        on=ROW_ID,
        how="right",
    ).drop(join_keys)


def impute_ratios(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Impute job role ratios by interpolation forward fill and backward fill.

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.
    """
    impute_groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date

    imputed_ratios = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .sort_by(order_key)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    impute_agg_lf = (
        estimated_job_role_posts_lf.group_by(impute_groups)
        .agg(
            # Sort the join key in the same manner as the imputed values.
            pl.col(EXPANDED_ID).sort_by(order_key),
            imputed_ratios,
        )
        .explode(EXPANDED_ID, IndCQC.imputed_ascwds_job_role_ratios)
        .drop(impute_groups)
    )

    return estimated_job_role_posts_lf.join(impute_agg_lf, on=EXPANDED_ID, how="left")


def get_percent_share_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
    input_col: str,
    output_col: str,
) -> pl.LazyFrame:
    """Calculate ratios over location and date using groupby-agg-explode pattern.

    Using groupby-agg-explode ensures it can be processed with the streaming engine.
    """
    groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]

    # Groupby-agg-explode on only necessary subset, before joining back on EXPANDED_ID.
    ratios_agg_lf = (
        estimated_job_role_posts_lf.group_by(groups)
        .agg(
            pl.col(EXPANDED_ID),  # Keep to align during explode
            percentage_share(input_col).cast(pl.Float32).alias(output_col),
        )
        .explode(EXPANDED_ID, output_col)
        # Drop groups to prevent duplicate columns after join.
        .drop(groups)
    )

    return estimated_job_role_posts_lf.join(ratios_agg_lf, on=EXPANDED_ID, how="left")


def get_job_counts_rolling_sum(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """ """
    rolling_groups = [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]
    # STEP A: Pre-aggregate down to monthly totals
    # (Shrinks 152M rows -> ~50k rows instantly via Hash Aggregation)
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_counts).sum()
    )

    # STEP B: Sort and roll on the small dataset.
    # This .sort() is completely safe because it's only operating on ~50k rows.
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(pl.col(IndCQC.imputed_ascwds_job_role_counts).sum().alias("rolling_sum"))
    )

    # STEP C: Join the rolling sum back to the main 152M row table
    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    )


def apply_manager_adjustments(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply the managerial adjustments."""
    pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    is_non_rm_manager = ManagerialFilledPostAdjustmentExpr._is_non_rm_manager()
    filled_posts = pl.col(IndCQC.estimate_filled_posts_by_job_role)

    stats_lf = estimated_job_role_posts_lf.group_by(pct_share_groups).agg(
        rm_diff=ManagerialFilledPostAdjustmentExpr._rm_manager_diff(),
        non_rm_total=(pl.when(is_non_rm_manager).then(filled_posts).sum()),
        non_rm_len=(pl.when(is_non_rm_manager).then(pl.lit(1)).sum()),
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        stats_lf,
        on=pct_share_groups,
        how="left",
    )

    return (
        estimated_job_role_posts_lf.with_columns(
            pct_share=pl.when(is_non_rm_manager).then(
                pl.when(pl.col("non_rm_total") == 0)
                .then(1 / pl.col("non_rm_len"))
                .otherwise(filled_posts / pl.col("non_rm_total"))
            )
        )
        .with_columns(
            pl.when(is_non_rm_manager)
            .then(
                filled_posts.add(pl.col("rm_diff").mul(pl.col("pct_share"))).clip(
                    lower_bound=0
                )
            )
            .when(ManagerialFilledPostAdjustmentExpr._is_registered_manager)
            .then(ManagerialFilledPostAdjustmentExpr._clip_rm_count())
            .otherwise(filled_posts)
            .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
        )
        .drop("pct_share", "rm_diff", "non_rm_total", "non_rm_len")
    )


def join_estimates_to_ascwds(
    estimates_lf: pl.LazyFrame,
    ascwds_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join job role estimates to ASCWDS counts ensuring a row for every job role.

    Performs a cross join on the estimates join keys first to ensure there is a row for
    every job role across all time periods for each location. This is then joined with
    the ASCWDS data.
    """
    join_keys = [
        IndCQC.ascwds_workplace_import_date,
        IndCQC.establishment_id,
    ]
    job_role_labels = IndCQC.main_job_role_clean_labelled

    # Narrow select "id" and join keys first to improve memory performance of
    # cross join. From this we get the full amount of rows expected.
    narrow_keys_lf = estimates_lf.select(["id"] + join_keys)
    # This is just a single column df with a row for each job role (~38).
    roles_lf = pl.LazyFrame(
        data=[AscwdsWorkerValueLabelsJobGroup.all_roles()],
        schema={job_role_labels: pl.Categorical},
    )
    # This will be the length of estimates dataset x number of job roles.
    expanded_keys_lf = narrow_keys_lf.join(roles_lf, how="cross")

    expanded_counts_lf = expanded_keys_lf.join(
        other=ascwds_lf,
        on=join_keys + [job_role_labels],
        how="left",
    )

    # Re-attach the wide base data - The streaming engine easily handles this
    # 1-to-many join via the 'id' column. Drop the join keys (used earlier) from
    # both sides as they are not relevant to the rest of the pipeline.
    return estimates_lf.join(
        expanded_counts_lf.drop(join_keys),
        on="id",
        how="right",
    ).drop(join_keys)


def impute_ratios(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Impute job role ratios by interpolation forward fill and backward fill.

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.
    """
    impute_groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date
    long_id = "long_id"

    imputed_ratios = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .sort_by(order_key)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    impute_agg_lf = (
        estimated_job_role_posts_lf.select(
            *impute_groups,
            order_key,
            long_id,
            IndCQC.ascwds_job_role_ratios,
        )
        .group_by(impute_groups)
        .agg(
            # Sort the join key in the same manner as the imputed values.
            pl.col(long_id).sort_by(order_key),
            imputed_ratios,
        )
        .explode(long_id, IndCQC.imputed_ascwds_job_role_ratios)
        .drop(impute_groups)
    )

    return estimated_job_role_posts_lf.join(impute_agg_lf, on=long_id, how="left")


def get_percent_share_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
    input_col: str,
    output_col: str,
) -> pl.LazyFrame:
    """Calculate ratios over location and date using groupby-agg-explode pattern.

    Using groupby-agg-explode ensures it can be processed with the streaming engine.
    """
    long_id: str = "long_id"
    groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]

    # Groupby-agg-explode on only necessary subset, before joining back on long_id.
    ratios_agg_lf = (
        estimated_job_role_posts_lf.select(*groups, long_id, input_col)
        .group_by(groups)
        .agg(
            pl.col(long_id),  # Keep to align during explode
            JRUtils.percentage_share(input_col).cast(pl.Float32).alias(output_col),
        )
        .explode(long_id, output_col)
        # Drop groups to prevent duplicate columns after join.
        .drop(groups)
    )

    return estimated_job_role_posts_lf.join(ratios_agg_lf, on=long_id, how="left")


def get_job_counts_rolling_sum(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """ """
    rolling_groups = [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]
    # STEP A: Pre-aggregate down to monthly totals
    # (Shrinks 152M rows -> ~50k rows instantly via Hash Aggregation)
    monthly_totals_lf = (
        estimated_job_role_posts_lf.select(
            *monthly_groups,
            IndCQC.imputed_ascwds_job_role_counts,
        )
        .group_by(monthly_groups)
        .agg(pl.col(IndCQC.imputed_ascwds_job_role_counts).sum())
    )

    # STEP B: Sort and roll on the small dataset.
    # This .sort() is completely safe because it's only operating on ~50k rows.
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(pl.col(IndCQC.imputed_ascwds_job_role_counts).sum().alias("rolling_sum"))
    )

    # STEP C: Join the rolling sum back to the main 152M row table
    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
    )


def log_polars_plan(lf: pl.LazyFrame, context: str) -> None:
    """Logs the explain plan and schema to CloudWatch immediately."""
    logger.info(f"--- PRE-FLIGHT CHECK: {context} ---")

    plan = lf.explain(engine="streaming")

    # We log line-by-line so CloudWatch doesn't truncate a massive single string.
    for line in plan.split("\n"):
        if line.strip():  # Skip empty lines
            logger.info(f"[PLAN] {line}")

    # Log the schema too.
    logger.info(f"Schema for {context}: {lf.collect_schema()}")

    logger.info(f"--- END PRE-FLIGHT PLAN: {context} ---")
    sys.stdout.flush()


def cast_to_schema(schema: dict[str, pl.DataType]) -> list[pl.Expr]:
    """Cast columns to given schema."""
    return [pl.col(c).cast(dtype) for c, dtype in schema.items()]


@contextmanager
def time_it(label: str):
    """Context manager to time code execution."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info(f"[METRIC] {label}: {elapsed:.4f}s")
        sys.stdout.flush()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--ascwds_job_role_counts_source",
            "Source s3 directory for parquet ASCWDS worker job role counts dataset",
        ),
        (
            "--estimates_by_job_role_destination",
            "Destination s3 directory",
        ),
    )
    main(
        estimates_source=args.estimates_source,
        ascwds_job_role_counts_source=args.ascwds_job_role_counts_source,
        estimates_by_job_role_destination=args.estimates_by_job_role_destination,
    )
