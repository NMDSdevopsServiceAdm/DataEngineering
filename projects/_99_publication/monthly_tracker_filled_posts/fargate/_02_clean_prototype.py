"""THROWAWAY diagnostics prototype - not part of the real pipeline (ticket 2201).

Runs the publication clean job under RunDiagnostics to compare ways of evaluating
the cleaned data once instead of six times. The variant is chosen by the
FIX_VARIANT environment variable:

    baseline              current logic, instrumented (reference)
    collect_rollup_input  collect job_role_enlarged_lf (post-aggregation)
    collect_cleaned       collect cleaned_lf (row level) in memory
    cache_rollup_input    .cache() on job_role_enlarged_lf

Delete this file (and its terraform / Step Function wiring) once the comparison
concludes.
"""

import os
from datetime import date

import polars as pl

from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    clean_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)
from utils.file_utils import split_s3_uri

VARIANTS = (
    "baseline",
    "collect_rollup_input",
    "collect_cleaned",
    "cache_rollup_input",
)
SAMPLE_INTERVAL_SECONDS = 2


def add_rows_for_publication_groups(
    cleaned_lf: pl.LazyFrame,
    publication_summary_lf: pl.LazyFrame,
    variant: str,
    diagnostics: RunDiagnostics,
) -> pl.LazyFrame:
    """
    Copy of clean_utils.add_rows_for_publication_groups with a per-variant hook on
    job_role_enlarged_lf, the frame consumed three times by the rollups.

    Args:
        cleaned_lf (pl.LazyFrame): location-level data.
        publication_summary_lf (pl.LazyFrame): aggregate_to_publication_rows output.
        variant (str): one of VARIANTS.
        diagnostics (RunDiagnostics): records plan checkpoints.

    Returns:
        pl.LazyFrame: publication_summary_lf with the rollup rows added.
    """
    publication_summary_lf = publication_summary_lf.with_columns(
        pl.col(IndCQC.primary_service_type).cast(pl.Categorical)
    )
    column_schema = publication_summary_lf.collect_schema()
    column_order = column_schema.names()

    metric_columns = [
        Pub.publication_filled_posts,
        Pub.publication_locationid_count,
        Pub.assessment_filled_posts_long_term,
        Pub.assessment_locationid_count_long_term,
        Pub.assessment_ct_total_employed_long_term,
        Pub.assessment_filled_posts_medium_term,
        Pub.assessment_locationid_count_medium_term,
        Pub.assessment_ct_total_employed_medium_term,
        Pub.assessment_filled_posts_short_term,
        Pub.assessment_locationid_count_short_term,
        Pub.assessment_ct_total_employed_short_term,
    ]

    all_job_roles_lf = (
        clean_utils.aggregate_to_publication_rows(
            cleaned_lf,
            group_keys=[
                IndCQC.cqc_location_import_date,
                IndCQC.current_region,
                IndCQC.primary_service_type,
            ],
        )
        .with_columns(
            pl.lit(PublishedJobGroupLabels.all_job_roles)
            .cast(column_schema[IndCQC.main_job_role_clean_labelled])
            .alias(IndCQC.main_job_role_clean_labelled),
            pl.col(IndCQC.primary_service_type).cast(pl.Categorical),
        )
        .select(column_order)
    )
    job_role_enlarged_lf = pl.concat(
        [publication_summary_lf, all_job_roles_lf], how="vertical"
    )

    if variant == "collect_rollup_input":
        job_role_enlarged_lf = job_role_enlarged_lf.collect(engine="streaming").lazy()
    elif variant == "cache_rollup_input":
        job_role_enlarged_lf = job_role_enlarged_lf.cache()
    # explain() on a bare .cache() frame panics in Polars 1.44.1, so skip the plan
    # for that variant here; the later checkpoints explain the whole plan.
    diagnostics.checkpoint(
        "after_rollup_input",
        None if variant == "cache_rollup_input" else job_role_enlarged_lf,
    )

    service_type_group_keys = [
        IndCQC.cqc_location_import_date,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.current_region,
    ]
    all_cqc_locations_lf = (
        job_role_enlarged_lf.group_by(service_type_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedMainService.all_locations)
            .cast(column_schema[IndCQC.primary_service_type])
            .alias(IndCQC.primary_service_type)
        )
        .select(column_order)
    )
    all_cqc_care_homes_lf = (
        job_role_enlarged_lf.filter(
            pl.col(IndCQC.primary_service_type).is_in(
                [
                    PrimaryServiceType.care_home_with_nursing,
                    PrimaryServiceType.care_home_only,
                ]
            )
        )
        .group_by(service_type_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedMainService.all_care_homes)
            .cast(column_schema[IndCQC.primary_service_type])
            .alias(IndCQC.primary_service_type)
        )
        .select(column_order)
    )
    service_type_enlarged_lf = pl.concat(
        [job_role_enlarged_lf, all_cqc_locations_lf, all_cqc_care_homes_lf],
        how="vertical",
    )

    england_group_keys = [
        IndCQC.cqc_location_import_date,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.primary_service_type,
    ]
    england_lf = (
        service_type_enlarged_lf.group_by(england_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedRegion.england)
            .cast(column_schema[IndCQC.current_region])
            .alias(IndCQC.current_region)
        )
        .select(column_order)
    )

    return pl.concat([service_type_enlarged_lf, england_lf], how="vertical")


def main(merge_data_source: str, clean_destination: str) -> None:
    """
    Runs the publication clean job for the FIX_VARIANT variant under RunDiagnostics.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): distinctly-named prototype output directory -
            never the real pipeline's destination

    Raises:
        ValueError: if FIX_VARIANT is not one of VARIANTS.
    """
    variant = os.environ.get("FIX_VARIANT", "")
    if variant not in VARIANTS:
        raise ValueError(f"FIX_VARIANT must be one of {VARIANTS}, got {variant!r}")

    data_bucket, _ = split_s3_uri(merge_data_source)
    diagnostics = RunDiagnostics(
        f"pub_02_clean_{variant}",
        data_bucket,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        merged_lf = utils.scan_parquet(merge_data_source)
        diagnostics.checkpoint("start", merged_lf)

        today = date.today()
        fy_year = today.year if today.month >= 4 else today.year - 1
        cutoff_date = date(fy_year - 6, 4, 1)
        cleaned_lf = merged_lf.filter(
            clean_utils.reduced_data_filter_expr(cutoff_date=cutoff_date)
        )

        cleaned_lf = cleaned_lf.with_columns(
            (pl.col(IndCQC.care_home_status_count) == 1).alias(Pub.consistent_service)
        )

        cleaned_lf = cleaned_lf.with_columns(
            pl.coalesce(
                IndCQC.ct_care_home_total_employed_imputed,
                IndCQC.ct_non_res_care_workers_employed_imputed,
            ).alias(Pub.ct_total_employed_imputed)
        )

        earliest_ct_data_date = date(2021, 7, 1)
        long_term_from_date = max(earliest_ct_data_date, cutoff_date)
        medium_term_from_date = date(fy_year - 1, 4, 1)
        short_term_from_date = date(fy_year, 4, 1)
        cleaned_lf = cleaned_lf.with_columns(
            clean_utils.has_continuous_data_since_date(
                Pub.ct_total_employed_imputed,
                long_term_from_date,
                Pub.ct_has_data_long_term,
            ),
            clean_utils.has_continuous_data_since_date(
                Pub.ct_total_employed_imputed,
                medium_term_from_date,
                Pub.ct_has_data_medium_term,
            ),
            clean_utils.has_continuous_data_since_date(
                Pub.ct_total_employed_imputed,
                short_term_from_date,
                Pub.ct_has_data_short_term,
            ),
        )

        ct_employed_columns = [
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        ]
        cleaned_lf = clean_utils.add_dispersion_filter(
            cleaned_lf,
            ct_employed_columns,
            long_term_from_date,
            Pub.ct_dispersion_filter_long_term,
        )
        cleaned_lf = clean_utils.add_dispersion_filter(
            cleaned_lf,
            ct_employed_columns,
            medium_term_from_date,
            Pub.ct_dispersion_filter_medium_term,
        )
        cleaned_lf = clean_utils.add_dispersion_filter(
            cleaned_lf,
            ct_employed_columns,
            short_term_from_date,
            Pub.ct_dispersion_filter_short_term,
        )

        if variant == "collect_cleaned":
            cleaned_lf = cleaned_lf.collect(engine="streaming").lazy()
        diagnostics.checkpoint("after_cleaned", cleaned_lf)

        publication_summary_lf = clean_utils.aggregate_to_publication_rows(cleaned_lf)
        publication_summary_lf = add_rows_for_publication_groups(
            cleaned_lf, publication_summary_lf, variant, diagnostics
        )

        group_columns = [
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        publication_summary_lf = publication_summary_lf.with_columns(
            clean_utils.calc_perc_change_between_rows(
                Pub.assessment_ct_total_employed_long_term,
                long_term_from_date,
                group_columns,
                Pub.assessment_ct_period_perc_change_long_term,
            ),
            clean_utils.calc_perc_change_between_rows(
                Pub.assessment_ct_total_employed_medium_term,
                medium_term_from_date,
                group_columns,
                Pub.assessment_ct_period_perc_change_medium_term,
            ),
            clean_utils.calc_perc_change_between_rows(
                Pub.assessment_ct_total_employed_short_term,
                short_term_from_date,
                group_columns,
                Pub.assessment_ct_period_perc_change_short_term,
            ),
            clean_utils.calc_perc_change_cumulative_from_given_period_onwards(
                Pub.assessment_ct_total_employed_long_term,
                long_term_from_date,
                group_columns,
                Pub.assessment_ct_cumulative_perc_change_long_term,
            ),
            clean_utils.calc_perc_change_cumulative_from_given_period_onwards(
                Pub.assessment_ct_total_employed_medium_term,
                medium_term_from_date,
                group_columns,
                Pub.assessment_ct_cumulative_perc_change_medium_term,
            ),
            clean_utils.calc_perc_change_cumulative_from_given_period_onwards(
                Pub.assessment_ct_total_employed_short_term,
                short_term_from_date,
                group_columns,
                Pub.assessment_ct_cumulative_perc_change_short_term,
            ),
        )
        diagnostics.checkpoint("after_rollups", publication_summary_lf)

        publication_summary_lf = publication_summary_lf.with_columns(
            clean_utils.format_large_number(
                Pub.publication_filled_posts, Pub.publication_filled_posts_formatted
            ),
            clean_utils.format_large_number(
                Pub.assessment_filled_posts_long_term,
                Pub.assessment_filled_posts_long_term_formatted,
            ),
            clean_utils.format_large_number(
                Pub.assessment_filled_posts_medium_term,
                Pub.assessment_filled_posts_medium_term_formatted,
            ),
            clean_utils.format_large_number(
                Pub.assessment_filled_posts_short_term,
                Pub.assessment_filled_posts_short_term_formatted,
            ),
            pl.col(IndCQC.cqc_location_import_date)
            .dt.strftime("%b %Y")
            .alias(Pub.cqc_location_import_date_abbreviated),
            pl.col(IndCQC.cqc_location_import_date)
            .dt.strftime("%B %Y")
            .alias(Pub.cqc_location_import_date_full),
        )
        diagnostics.checkpoint("before_sink", publication_summary_lf)

        utils.sink_to_parquet(
            lazy_df=publication_summary_lf,
            output_path=clean_destination,
        )
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merge_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--clean_destination",
            "Distinctly-named prototype output directory",
        ),
    )
    main(args.merge_data_source, args.clean_destination)
