import logging
import sys

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
        estimated_posts_lf = estimated_posts_lf.with_columns().join(
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
                {IndCQC.ascwds_worker_import_date: IndCQC.ascwds_workplace_import_date}
            )
        )

        estimated_job_role_posts_lf = estimated_posts_lf.join(
            other=ascwds_job_role_counts_lf,
            on=join_keys,
            how="left",
        )

    estimated_job_role_posts_lf = JRUtils.nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    ).drop(
        IndCQC.estimate_filled_posts_source,
        IndCQC.ascwds_filled_posts_dedup_clean,
    )

    pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    log_polars_plan(estimated_job_role_posts_lf, "Post Join")
    # Sort, gather to struct, explain then save to a temp file.
    tmp_dest = estimates_by_job_role_destination.replace("dataset=", "dataset=temp_")
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.sort(pct_share_groups)
    cols = estimated_job_role_posts_lf.collect_schema().names()
    struct_cols = [IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_counts]
    metadata_cols = [c for c in cols if c not in struct_cols + pct_share_groups]
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.group_by(
        pct_share_groups
    ).agg(
        *[pl.col(c).first() for c in metadata_cols],
        pl.struct(struct_cols).alias("job_role_data"),
    )
    log_polars_plan(estimated_job_role_posts_lf, "After collecting to struct")
    tmp_file = f"{tmp_dest}file.parquet"
    estimated_job_role_posts_lf.sink_parquet(tmp_file, mkdir=True, engine="streaming")
    estimated_job_role_posts_lf = (
        pl.scan_parquet(tmp_file, cache=False)
        .explode("job_role_data")
        .unnest("job_role_data")
        .drop(join_keys)
    )

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


def cast_to_schema(schema: dict[str, pl.DataType]) -> list[pl.Expr]:
    """Cast columns to given schema."""
    return [pl.col(c).cast(dtype) for c, dtype in schema.items()]


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
