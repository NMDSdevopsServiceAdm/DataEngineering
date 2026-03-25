import logging
import os
import sys
import time
from contextlib import contextmanager

import polars as pl
import polars.selectors as cs

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as JRUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Restrict vCPU count so memory isn't throttled (8 available on ECS).
os.environ["POLARS_MAX_THREADS"] = "4"

# ECS/Cloudwatch captures stdout logging.
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

logging.info(f"Polars temp dir set at: {os.environ['POLARS_TEMP_DIR']}")
logging.info(f"Polars max threads set at: {os.environ['POLARS_MAX_THREADS']}")

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
    IndCQC.estimate_filled_posts: pl.Float64,
    IndCQC.estimate_filled_posts_source: pl.Categorical,
    IndCQC.primary_service_type: pl.Categorical,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float64,
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
            estimated_posts_lf = (
                utils.scan_parquet(
                    source=estimates_source,
                    selected_columns=list(transformation_columns),
                )
                # This row index will be used to join back metadata.
                .with_row_index(name="id").with_columns(
                    cast_to_schema(transformation_columns)
                )
            )

            # Cartesian product all roles before joining ASCWDS.
            estimated_posts_lf = estimated_posts_lf.join(
                pl.LazyFrame(
                    data=[AscwdsWorkerValueLabelsJobGroup.all_roles()],
                    schema={IndCQC.main_job_role_clean_labelled: pl.Categorical},
                ),
                how="cross",
            )

            ascwds_job_role_counts_lf = (
                utils.scan_parquet(
                    source=ascwds_job_role_counts_source,
                    selected_columns=list(ascwds_columns_to_import),
                )
                .with_columns(cast_to_schema(ascwds_columns_to_import))
                .rename(
                    {
                        IndCQC.ascwds_worker_import_date: IndCQC.ascwds_workplace_import_date
                    }
                )
            )

            estimated_job_role_posts_lf = estimated_posts_lf.join(
                other=ascwds_job_role_counts_lf,
                on=join_keys + [IndCQC.main_job_role_clean_labelled],
                how="left",
            )

            estimated_job_role_posts_lf = (
                JRUtils.nullify_job_role_count_when_source_not_ascwds(
                    estimated_job_role_posts_lf
                ).drop(
                    IndCQC.estimate_filled_posts_source,
                    IndCQC.ascwds_filled_posts_dedup_clean,
                )
            )

            pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
            log_polars_plan(estimated_job_role_posts_lf, "Post Join")
            # Sink to temp, then rescan to reset Lazy pipeline.
            tmp_dest = estimates_by_job_role_destination.replace(
                "dataset=", "dataset=temp_"
            )
            tmp_file = f"{tmp_dest}file.parquet"
            estimated_job_role_posts_lf.sink_parquet(
                tmp_file, mkdir=True, engine="streaming"
            )

        with time_it("group-by-agg"):
            estimated_job_role_posts_lf = pl.scan_parquet(tmp_file, cache=False).drop(
                join_keys
            )
            job_role_ratios = JRUtils.percentage_share(
                IndCQC.ascwds_job_role_counts
            ).alias(IndCQC.ascwds_job_role_ratios)
            estimated_job_role_posts_lf = estimated_job_role_posts_lf.sort(
                pct_share_groups
            )
            groupby_agg_lf = (
                estimated_job_role_posts_lf.group_by(pct_share_groups)
                .agg(pl.all().exclude(pct_share_groups), job_role_ratios)
                .explode(cs.all() - cs.by_name(pct_share_groups))
            )

            log_polars_plan(groupby_agg_lf, "Groupby agg pct share")
            tmp_dest = estimates_by_job_role_destination.replace(
                "dataset=", "dataset=temp1_"
            )
            tmp_file = f"{tmp_dest}file.parquet"
            groupby_agg_lf.sink_parquet(tmp_file, mkdir=True, engine="streaming")

        # Do linear interpolation, then forward fill and backward fill to get a full
        # time series for each job role and location.
        with time_it("impute_ratios"):
            estimated_job_role_posts_lf = pl.scan_parquet(tmp_file, cache=False)
            groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
            order_key = IndCQC.cqc_location_import_date
            imputed_ratios = (
                pl.col(IndCQC.ascwds_job_role_ratios)
                .interpolate()
                .forward_fill()
                .backward_fill()
                .alias(IndCQC.imputed_ascwds_job_role_ratios)
            )
            estimated_job_role_posts_lf = (
                estimated_job_role_posts_lf.sort(*groups, order_key)
                .group_by(groups)
                .agg(pl.all().exclude(groups), imputed_ratios)
                .explode(cs.all() - cs.by_name(groups))
            )

            # Multiply imputed ratios by estimate filled posts to get counts.
            estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
                pl.col(IndCQC.estimate_filled_posts)
                .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
                .alias(IndCQC.imputed_ascwds_job_role_counts)
            )

            log_polars_plan(estimated_job_role_posts_lf, "groupby-impute-agg")
            tmp_dest = estimates_by_job_role_destination.replace(
                "dataset=", "dataset=temp2_"
            )
            tmp_file = f"{tmp_dest}file.parquet"
            estimated_job_role_posts_lf.sink_parquet(
                tmp_file, mkdir=True, engine="streaming"
            )

        # Get the proportions of the rolling sum of job counts within each location.
        with time_it("rolling_ratios_and_coalesce"):
            estimated_job_role_posts_lf = pl.scan_parquet(tmp_file, cache=False)
            rolling_groups = [
                IndCQC.primary_service_type,
                IndCQC.main_job_role_clean_labelled,
            ]
            rolling_sum_counts = (
                pl.col(IndCQC.imputed_ascwds_job_role_counts)
                .sum()
                .rolling(order_key, period="6mo")
                .alias("rolling_sum")
            )
            estimated_job_role_posts_lf = (
                estimated_job_role_posts_lf.sort(*rolling_groups, order_key)
                .group_by(rolling_groups)
                .agg(pl.all().exclude(rolling_groups), rolling_sum_counts)
                .explode(cs.all() - cs.by_name(rolling_groups))
            )
            rolling_ratios = JRUtils.percentage_share("rolling_sum").alias(
                IndCQC.ascwds_job_role_rolling_ratio
            )
            estimated_job_role_posts_lf = (
                estimated_job_role_posts_lf.sort(pct_share_groups)
                .group_by(pct_share_groups)
                .agg(pl.all().exclude(pct_share_groups), rolling_ratios)
                .explode(cs.all() - cs.by_name(pct_share_groups))
            )

            estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
                utils.coalesce_with_source_labels(
                    cols=[
                        # TODO: Uncomment when filtered column is created.
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

            log_polars_plan(estimated_job_role_posts_lf, "rolling_ratios_and_coalesce")
            tmp_dest = estimates_by_job_role_destination.replace(
                "dataset=", "dataset=temp3_"
            )
            tmp_file = f"{tmp_dest}file.parquet"
            estimated_job_role_posts_lf.sink_parquet(
                tmp_file, mkdir=True, engine="streaming"
            )
        with time_it("manager_adjustments"):
            estimated_job_role_posts_lf = pl.scan_parquet(tmp_file, cache=False)
            is_non_rm_manager = (
                JRUtils.ManagerialFilledPostAdjustmentExpr._is_non_rm_manager()
            )
            filled_posts = pl.col(IndCQC.estimate_filled_posts_by_job_role)
            # Calculate the grouped stats first and then join back.
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

            sum_all_job_roles = (
                pl.sum(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted).alias(
                    IndCQC.estimate_filled_posts_from_all_job_roles
                ),
            )
            estimated_job_role_posts_lf = (
                estimated_job_role_posts_lf.sort(pct_share_groups)
                .group_by(pct_share_groups)
                .agg(pl.all().exclude(pct_share_groups), sum_all_job_roles)
                .explode(cs.all() - cs.by_name(pct_share_groups))
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
