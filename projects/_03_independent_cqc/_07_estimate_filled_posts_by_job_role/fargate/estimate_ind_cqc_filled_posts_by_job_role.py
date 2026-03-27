import logging
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as JRUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# ECS/Cloudwatch captures stdout logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

polars_temp_dir = os.getenv("POLARS_TEMP_DIR", tempfile.gettempdir())
logging.info(f"Polars temp dir set at: {polars_temp_dir}")
CHECKPOINT_PATH = Path(polars_temp_dir) / "checkpoints"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

partition_keys = [Keys.year]

estimates_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_registration_date,
    IndCQC.registered_manager_names,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
ascwds_columns_to_import = {
    IndCQC.ascwds_worker_import_date: pl.Date,
    IndCQC.establishment_id: pl.Categorical,
    IndCQC.main_job_role_clean_labelled: pl.Categorical,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}
transformation_columns = {
    IndCQC.location_id: pl.Categorical,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.establishment_id: pl.Categorical,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.estimate_filled_posts_source: pl.Categorical,
    IndCQC.primary_service_type: pl.Categorical,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
    Keys.year: pl.Int16,
}
join_keys = [
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
]
dropped_columns = [
    IndCQC.establishment_id,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.ascwds_worker_import_date,
    IndCQC.estimate_filled_posts_source,
    IndCQC.ascwds_filled_posts_dedup_clean,
]


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
    # Needed so we can join on Categorical columns.
    with pl.StringCache():
        with time_it("scan_and_join"):
            estimated_posts_base_lf = (
                pl.scan_parquet(estimates_source, low_memory=True)
                .select(list(transformation_columns))
                .with_row_index(name="id")
                .with_columns(cast_to_schema(transformation_columns))
            )

            # Subset: Extract ONLY the keys needed for the cross join and ASCWDS join.
            narrow_keys_lf = estimated_posts_base_lf.select(["id"] + join_keys)

            # Expand: Perform the massive cross-join on the tiny subset
            roles_lf = pl.LazyFrame(
                data=[AscwdsWorkerValueLabelsJobGroup.all_roles()],
                schema={IndCQC.main_job_role_clean_labelled: pl.Categorical},
            )
            expanded_keys_lf = narrow_keys_lf.join(roles_lf, how="cross")

            # Prepare ASCWDS data
            col_name_map = {
                IndCQC.ascwds_worker_import_date: IndCQC.ascwds_workplace_import_date
            }
            ascwds_job_role_counts_lf = (
                utils.scan_parquet(
                    source=ascwds_job_role_counts_source,
                    selected_columns=list(ascwds_columns_to_import),
                )
                .with_columns(cast_to_schema(ascwds_columns_to_import))
                .rename(col_name_map)
            )

            # Join ASCWDS counts onto the expanded narrow subset
            expanded_counts_lf = expanded_keys_lf.join(
                other=ascwds_job_role_counts_lf,
                on=join_keys + [IndCQC.main_job_role_clean_labelled],
                how="left",
            )

            # Join Back: Re-attach the wide base data right before processing logic
            # The streaming engine easily handles this 1-to-many join via the 'id' column
            estimated_job_role_posts_lf = estimated_posts_base_lf.join(
                expanded_counts_lf,
                on="id",
                how="right",
            ).drop(join_keys)

            estimated_job_role_posts_lf = (
                JRUtils.nullify_job_role_count_when_source_not_ascwds(
                    estimated_job_role_posts_lf
                ).drop(
                    IndCQC.estimate_filled_posts_source,
                    IndCQC.ascwds_filled_posts_dedup_clean,
                )
            )

            long_id: str = "long_id"
            estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_row_index(
                long_id
            )

            log_polars_plan(estimated_job_role_posts_lf, "Post Join")
            checkpoint_filepath = CHECKPOINT_PATH / "checkpoint1.parquet"

            estimated_job_role_posts_lf.sink_parquet(
                checkpoint_filepath,
                mkdir=True,
                engine="streaming",
            )

        with time_it("Final pipeline"):
            estimated_job_role_posts_lf = pl.scan_parquet(
                checkpoint_filepath,
                low_memory=True,
            )

            pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
            job_role_col = IndCQC.main_job_role_clean_labelled

            # ---------------------------------------------------------
            # 1. Job Role Ratios
            # ---------------------------------------------------------
            job_role_ratios = JRUtils.percentage_share(IndCQC.ascwds_job_role_counts)

            ratios_agg_lf = (
                estimated_job_role_posts_lf.select(
                    *pct_share_groups,
                    long_id,
                    IndCQC.ascwds_job_role_counts,
                )
                .group_by(pct_share_groups)
                .agg(
                    pl.col(long_id),  # Keep to align during explode
                    job_role_ratios.alias(IndCQC.ascwds_job_role_ratios),
                )
                .explode(long_id, IndCQC.ascwds_job_role_ratios)
                .drop(pct_share_groups)
            )

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                ratios_agg_lf,
                on=long_id,
                how="left",
            )

            # ---------------------------------------------------------
            # 2. Imputation (Interpolate, FF, BF)
            # ---------------------------------------------------------
            impute_groups = [IndCQC.location_id, job_role_col]
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

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                impute_agg_lf,
                on=long_id,
                how="left",
            )

            # Multiply imputed ratios by estimate filled posts
            estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
                pl.col(IndCQC.estimate_filled_posts)
                .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
                .alias(IndCQC.imputed_ascwds_job_role_counts)
            )

            # ---------------------------------------------------------
            # 3. Rolling Sum and Rolling Ratios
            # ---------------------------------------------------------

            rolling_groups = [IndCQC.primary_service_type, job_role_col]
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

            # STEP B: Sort and roll on the TINY dataset
            # This .sort() is completely safe because it's only operating on ~50k rows
            rolling_agg_lf = (
                monthly_totals_lf.sort(*rolling_groups, order_key)
                .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
                .agg(
                    pl.col(IndCQC.imputed_ascwds_job_role_counts)
                    .sum()
                    .alias("rolling_sum")
                )
            )

            # STEP C: Join the rolling sum back to the main 152M row table
            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                rolling_agg_lf,
                on=monthly_groups,
                how="left",
            )

            rolling_ratios_agg_lf = (
                estimated_job_role_posts_lf.select(
                    *pct_share_groups,
                    long_id,
                    "rolling_sum",
                )
                .group_by(pct_share_groups)
                .agg(
                    pl.col(long_id),
                    JRUtils.percentage_share("rolling_sum").alias(
                        IndCQC.ascwds_job_role_rolling_ratio
                    ),
                )
                .explode(long_id, IndCQC.ascwds_job_role_rolling_ratio)
                .drop(pct_share_groups)
            )

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                rolling_ratios_agg_lf,
                on=long_id,
                how="left",
            ).drop("rolling_sum")

            # ---------------------------------------------------------
            # Coalesce & Multiply
            # ---------------------------------------------------------
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

            # ---------------------------------------------------------
            # Manager Adjustment
            # ---------------------------------------------------------
            is_non_rm_manager = (
                JRUtils.ManagerialFilledPostAdjustmentExpr._is_non_rm_manager()
            )
            filled_posts = pl.col(IndCQC.estimate_filled_posts_by_job_role)

            stats_lf = estimated_job_role_posts_lf.group_by(pct_share_groups).agg(
                rm_diff=JRUtils.ManagerialFilledPostAdjustmentExpr._rm_manager_diff(),
                non_rm_total=(pl.when(is_non_rm_manager).then(filled_posts).sum()),
                non_rm_len=(pl.when(is_non_rm_manager).then(pl.lit(1)).sum()),
            )

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                stats_lf,
                on=pct_share_groups,
                how="left",
            )

            estimated_job_role_posts_lf = (
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
                        filled_posts.add(
                            pl.col("rm_diff").mul(pl.col("pct_share"))
                        ).clip(lower_bound=0)
                    )
                    .when(
                        JRUtils.ManagerialFilledPostAdjustmentExpr._is_registered_manager
                    )
                    .then(JRUtils.ManagerialFilledPostAdjustmentExpr._clip_rm_count())
                    .otherwise(filled_posts)
                    .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
                )
                .drop("pct_share", "rm_diff", "non_rm_total", "non_rm_len")
            )

            # ---------------------------------------------------------
            # 4. Final Sum (Scalar Aggregation)
            # ---------------------------------------------------------
            sum_all_job_roles = pl.sum(
                IndCQC.estimate_filled_posts_by_job_role_manager_adjusted
            ).alias(IndCQC.estimate_filled_posts_from_all_job_roles)

            # Since this is a scalar reduction per group, no explode is needed.
            final_sum_agg_lf = estimated_job_role_posts_lf.group_by(
                pct_share_groups
            ).agg(sum_all_job_roles)

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
                final_sum_agg_lf,
                on=pct_share_groups,
                how="left",
            )

            log_polars_plan(estimated_job_role_posts_lf, "Final Transformation")

            utils.sink_to_parquet(
                lazy_df=estimated_job_role_posts_lf,
                output_path=estimates_by_job_role_destination,
                partition_cols=partition_keys,
                append=False,
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
