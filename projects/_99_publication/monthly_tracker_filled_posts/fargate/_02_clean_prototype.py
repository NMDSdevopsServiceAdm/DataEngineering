"""THROWAWAY. Instrumented copy of _02_clean.py for the 2102 monthly-data spike.

Mirrors _02_clean.main exactly, with a RunDiagnostics checkpoint either side of each
step, so the RSS curve can be attributed to a stage rather than to the job as a whole.
The open question is whether retaining full monthly data back to 2015 (instead of the
last 2 financial years) pushes peak memory near the task's ceiling.

Writes to its own dataset name so it can never overwrite the real pipeline's output.
Delete this file, its Dockerfile COPY line, its terraform module and its step function
definition once the investigation concludes.
"""

from datetime import date

import polars as pl

from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from polars_utils.utils import split_s3_uri
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    clean_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

SAMPLE_INTERVAL_SECONDS: float = 10

MONTHLY_DATA_FROM_DATE = date(2015, 1, 1)


def main(
    merge_data_source: str,
    clean_destination: str,
) -> None:
    """
    Instrumented copy of the monthly tracker publication clean step.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): destination s3 directory for the cleaned data
    """
    data_bucket, _ = split_s3_uri(clean_destination)
    diagnostics = RunDiagnostics(
        "monthly_tracker_02_clean_prototype",
        data_bucket,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        cleaned_lf = utils.scan_parquet(merge_data_source).filter(
            pl.col(IndCQC.cqc_location_import_date) >= MONTHLY_DATA_FROM_DATE
        )
        diagnostics.checkpoint("after_filter", cleaned_lf)

        # cutoff_date/fy_year compute the long/medium/short term assessment window
        # boundaries below - unrelated to the retention filter above.
        today = date.today()
        fy_year = today.year if today.month >= 4 else today.year - 1
        cutoff_date = date(fy_year - 6, 4, 1)

        cleaned_lf = cleaned_lf.with_columns(
            (pl.col(IndCQC.care_home_status_count) == 1).alias(Pub.consistent_service)
        )

        cleaned_lf = cleaned_lf.with_columns(
            pl.coalesce(
                IndCQC.ct_care_home_total_employed_imputed,
                IndCQC.ct_non_res_care_workers_employed_imputed,
            ).alias(Pub.ct_total_employed_imputed)
        )

        # 1 July 2021 is the earliest date this CT data exists, so long_term_from_date
        # stays pinned there rather than rolling forward every year - unless/until the
        # retention cutoff_date itself moves past it, at which point that takes over.
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
        diagnostics.checkpoint("after_has_data_columns", cleaned_lf)

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
        diagnostics.checkpoint("after_dispersion_filters", cleaned_lf)

        publication_summary_lf = clean_utils.aggregate_to_publication_rows(cleaned_lf)
        publication_summary_lf = clean_utils.add_rows_for_publication_groups(
            cleaned_lf, publication_summary_lf
        )
        diagnostics.checkpoint("after_aggregation", publication_summary_lf)

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
        diagnostics.checkpoint("after_perc_change_columns", publication_summary_lf)

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
            "Destination s3 directory for the cleaned data",
        ),
    )
    main(args.merge_data_source, args.clean_destination)
