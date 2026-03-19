import logging
import sys

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as JRUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

# ECS/Cloudwatch captures stdout logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


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
ascwds_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.ascwds_job_role_counts,
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
    estimated_posts_lf = utils.scan_parquet(
        source=estimates_source,
        selected_columns=estimates_columns_to_import,
    ).with_row_index(name="id")
    log_nrows(estimated_posts_lf, "estimated_posts")

    ascwds_job_role_counts_lf = utils.scan_parquet(
        source=ascwds_job_role_counts_source,
        selected_columns=ascwds_columns_to_import,
    )
    log_nrows(estimated_posts_lf, "ascwds_job_role_counts")

    estimated_job_role_posts_lf = JRUtils.join_worker_to_estimates_dataframe(
        estimated_posts_lf, ascwds_job_role_counts_lf
    )
    log_nrows(estimated_job_role_posts_lf, "after join")

    estimated_job_role_posts_lf = JRUtils.nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    )

    pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        JRUtils.percentage_share(IndCQC.ascwds_job_role_counts)
        .over(pct_share_groups)
        .alias(IndCQC.ascwds_job_role_ratios)
    )
    # Do linear interpolation, then forward fill and backward fill to get a full
    # time series for each job role and location.
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        JRUtils.impute_full_time_series(IndCQC.ascwds_job_role_ratios)
        .over(
            [IndCQC.location_id, IndCQC.main_job_role_clean_labelled],
            order_by=IndCQC.cqc_location_import_date,
        )
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    # Multiply imputed ratios by estimate filled posts to get counts.
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.estimate_filled_posts)
        .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
        .alias(IndCQC.imputed_ascwds_job_role_counts)
    )
    # Get the proportions of the rolling sum of job counts within each location.
    estimated_job_role_posts_lf = (
        estimated_job_role_posts_lf.with_columns(
            JRUtils.rolling_sum_of_job_role_counts(period="6mo").alias(
                IndCQC.ascwds_job_role_rolling_sum
            )
        )
        .with_columns(
            JRUtils.percentage_share(IndCQC.ascwds_job_role_rolling_sum)
            .over(pct_share_groups)
            .alias(IndCQC.ascwds_job_role_rolling_ratio)
        )
        .drop(IndCQC.ascwds_job_role_rolling_sum)
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

    adjustment_expr = JRUtils.ManagerialFilledPostAdjustmentExpr.build()
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        adjustment_expr.over(pct_share_groups).alias(
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted
        )
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.sum(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
        .over(pct_share_groups)
        .alias(IndCQC.estimate_filled_posts_from_all_job_roles)
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


def log_nrows(lf: pl.LazyFrame, context: str) -> None:
    """Logs the count of rows to CloudWatch immediately."""
    logger.info(f"nrows - {context}: {lf.select(pl.len()).collect().item()}")
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
