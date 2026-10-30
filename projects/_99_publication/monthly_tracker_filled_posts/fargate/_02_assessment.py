import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_sheet_text,
    assessment_workbook_utils,
    merge_utils,
    publication_utils,
    run_log_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

RUN_PARTITION_COLUMNS = [ArchiveKeys.archive_date, ArchiveKeys.run_number]


def main(
    prep_source: str,
    assessment_destination: str,
    run_log_source: str,
    workbook_destination: str,
    run_number: int | None = None,
) -> None:
    """
    Assesses one prepared run: summarises it for publication, adds the change over
    time and against the previously published run, then saves the assessment data
    and a filled assessment workbook.

    Args:
        prep_source (str): s3 directory holding the prepared data of every run
        assessment_destination (str): destination s3 directory for the assessment data,
            partitioned by archive_date and run_number
        run_log_source (str): s3 directory of the archive run log, used to find the
            previously published run
        workbook_destination (str): s3 directory for the filled assessment workbook,
            named by archive_date and run_number
        run_number (int | None): the prepared run to use. Defaults to None, the latest run
    """
    run_source = merge_utils.resolve_run_sources([prep_source], run_number)[0]
    print(f"Assessing prepared run: {run_source}")
    assessed_run_number = merge_utils.run_number_from_source(run_source)
    prepared_lf = utils.scan_parquet(run_source)

    prepared_lf = prepared_lf.with_columns(
        pl.coalesce(
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        ).alias(Pub.ct_total_employed_imputed)
    )

    today = date.today()
    fy_year = today.year if today.month >= 4 else today.year - 1
    # The prep step already applied the retention filter, so the earliest prepared
    # import date is the cutoff_date this run was prepared with.
    cutoff_date = (
        prepared_lf.select(pl.col(IndCQC.cqc_location_import_date).min())
        .collect()
        .item()
    )

    # 1 July 2021 is the earliest date this CT data exists, so long_term_from_date
    # stays pinned there rather than rolling forward every year - unless/until the
    # retention cutoff_date itself moves past it, at which point that takes over.
    # long_term_from_date must report on March, June, September or December, as the
    # workbook's long term sheets only show those periods.
    earliest_ct_data_date = date(2021, 7, 1)
    long_term_from_date = max(earliest_ct_data_date, cutoff_date)
    medium_term_from_date = date(fy_year - 1, 4, 1)
    short_term_from_date = date(fy_year, 4, 1)
    prepared_lf = prepared_lf.with_columns(
        publication_utils.has_consistent_service_since_date(
            long_term_from_date, Pub.consistent_service_long
        ),
        publication_utils.has_consistent_service_since_date(
            medium_term_from_date, Pub.consistent_service_medium
        ),
        publication_utils.has_consistent_service_since_date(
            short_term_from_date, Pub.consistent_service_short
        ),
        publication_utils.has_continuous_data_since_date(
            Pub.ct_total_employed_imputed,
            long_term_from_date,
            Pub.ct_has_data_long,
        ),
        publication_utils.has_continuous_data_since_date(
            Pub.ct_total_employed_imputed,
            medium_term_from_date,
            Pub.ct_has_data_medium,
        ),
        publication_utils.has_continuous_data_since_date(
            Pub.ct_total_employed_imputed,
            short_term_from_date,
            Pub.ct_has_data_short,
        ),
    )

    ct_employed_columns = [
        IndCQC.ct_care_home_total_employed_imputed,
        IndCQC.ct_non_res_care_workers_employed_imputed,
    ]
    prepared_lf = publication_utils.add_dispersion_filter(
        prepared_lf,
        ct_employed_columns,
        long_term_from_date,
        Pub.ct_dispersion_filter_long,
    )
    prepared_lf = publication_utils.add_dispersion_filter(
        prepared_lf,
        ct_employed_columns,
        medium_term_from_date,
        Pub.ct_dispersion_filter_medium,
    )
    prepared_lf = publication_utils.add_dispersion_filter(
        prepared_lf,
        ct_employed_columns,
        short_term_from_date,
        Pub.ct_dispersion_filter_short,
    )

    assessment_summary_lf = publication_utils.aggregate_to_publication_rows(prepared_lf)
    assessment_summary_lf = publication_utils.add_rows_for_publication_groups(
        prepared_lf, assessment_summary_lf
    )

    group_columns = [
        IndCQC.main_job_role_clean_labelled,
        IndCQC.current_region,
        IndCQC.primary_service_type,
    ]
    assessment_summary_lf = assessment_summary_lf.with_columns(
        publication_utils.calc_perc_change_from_previous_april(
            Pub.unfiltered_filled_posts,
            group_columns,
            Pub.unfiltered_filled_posts_april_pct_change,
        ),
        publication_utils.calc_perc_change_from_previous_april(
            Pub.unfiltered_locationid_count,
            group_columns,
            Pub.unfiltered_locationid_count_april_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.unfiltered_filled_posts,
            short_term_from_date,
            group_columns,
            Pub.unfiltered_short_sfc_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.unfiltered_locationid_count,
            short_term_from_date,
            group_columns,
            Pub.unfiltered_short_locationid_count_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_long_sfc_filled_posts,
            long_term_from_date,
            group_columns,
            Pub.filtered_long_sfc_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_long_ct_total_employed,
            long_term_from_date,
            group_columns,
            Pub.filtered_long_ct_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_medium_sfc_filled_posts,
            medium_term_from_date,
            group_columns,
            Pub.filtered_medium_sfc_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_medium_ct_total_employed,
            medium_term_from_date,
            group_columns,
            Pub.filtered_medium_ct_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_short_sfc_filled_posts,
            short_term_from_date,
            group_columns,
            Pub.filtered_short_sfc_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.filtered_short_ct_total_employed,
            short_term_from_date,
            group_columns,
            Pub.filtered_short_ct_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.unfiltered_filled_posts,
            long_term_from_date,
            group_columns,
            Pub.unfiltered_long_sfc_cum_pct_change,
        ),
        publication_utils.calc_perc_change_cumulative_from_given_period_onwards(
            Pub.unfiltered_long_ct_total_employed,
            long_term_from_date,
            group_columns,
            Pub.unfiltered_long_ct_cum_pct_change,
        ),
    )

    prev_run_number = run_log_utils.get_previous_published_run_number(
        run_log_source, assessed_run_number
    )
    if prev_run_number is None:
        print("No previously published run found; previous run columns are null")
        prev_assessment_lf = None
    else:
        prev_run_source = merge_utils.resolve_run_sources(
            [assessment_destination], prev_run_number
        )[0]
        print(f"Comparing to previously published run: {prev_run_source}")
        prev_assessment_lf = utils.scan_parquet(prev_run_source)
    assessment_summary_lf = publication_utils.add_prev_run_filled_posts_columns(
        assessment_summary_lf, prev_assessment_lf
    )

    assessment_summary_lf = assessment_summary_lf.with_columns(
        publication_utils.period_label_short_expr(),
        publication_utils.period_label_long_expr(),
    )

    utils.sink_to_parquet(
        lazy_df=merge_utils.add_run_partition_columns(
            assessment_summary_lf, run_source
        ),
        output_path=assessment_destination,
        partition_cols=RUN_PARTITION_COLUMNS,
    )

    # Read back the small written run so the plan above isn't computed twice.
    run_assessment_lf = utils.scan_parquet(
        merge_utils.run_partition_path(assessment_destination, run_source)
    )
    workbook_name = (
        f"monthly_tracking_summary_{merge_utils.archive_date_from_source(run_source)}"
        f"_{assessed_run_number}.xlsx"
    )
    with tempfile.TemporaryDirectory() as workbook_folder:
        workbook_path = Path(workbook_folder) / workbook_name
        assessment_workbook_utils.fill_assessment_workbook_from_lf(
            run_assessment_lf,
            cell_map.ASSESSMENT_SHEETS,
            assessment_sheet_text.TermFromDates(
                short=short_term_from_date,
                medium=medium_term_from_date,
                long=long_term_from_date,
            ),
            assessed_run_number,
            cell_map.TEMPLATE_PATH,
            workbook_path,
        )
        assessment_workbook_utils.upload_to_s3(workbook_path, workbook_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--prep_source",
            "S3 directory holding the prepared data of every run",
        ),
        (
            "--assessment_destination",
            "Destination s3 directory for the assessment data",
        ),
        (
            "--run_log_source",
            "S3 directory of the archive run log",
        ),
        (
            "--workbook_destination",
            "Destination s3 directory for the filled assessment workbook",
        ),
        (
            "--run_number",
            "Prepared run number to use, or 'latest'",
            False,
            merge_utils.LATEST_RUN,
        ),
    )
    main(
        prep_source=args.prep_source,
        assessment_destination=args.assessment_destination,
        run_log_source=args.run_log_source,
        workbook_destination=args.workbook_destination,
        run_number=merge_utils.parse_run_number(args.run_number),
    )
