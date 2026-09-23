from datetime import date

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    clean_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub


def main(
    merge_data_source: str,
    clean_destination: str,
) -> None:
    """
    Cleans merged job role data.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): destination s3 directory for the cleaned data
    """
    cleaned_lf = utils.scan_parquet(merge_data_source)

    # today/fy_year/cutoff_date are kept only to compute the long/medium/short
    # term assessment window boundaries below - they no longer filter any rows.
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

    publication_summary_lf = clean_utils.aggregate_to_publication_rows(cleaned_lf)
    publication_summary_lf = clean_utils.add_rows_for_publication_groups(
        cleaned_lf, publication_summary_lf
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

    utils.sink_to_parquet(
        lazy_df=publication_summary_lf,
        output_path=clean_destination,
    )


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
