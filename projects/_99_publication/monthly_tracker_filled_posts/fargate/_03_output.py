from datetime import date

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    publication_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PublishedJobGroupLabels

FINANCIAL_YEAR_START_MONTH = 4


def main(
    assessment_source: str,
    output_destination: str,
    run_number: int,
) -> None:
    """
    Builds the public data for one selected assessed run: the "All job roles"
    rows, one per year historically and monthly for the current financial
    year, with the columns of the T0, T1 and T2 data-download tables.

    Args:
        assessment_source (str): s3 directory holding the assessed data of every run
        output_destination (str): destination s3 directory for the publication data
        run_number (int): the assessed run to publish
    """
    run_source = merge_utils.resolve_run_sources([assessment_source], run_number)[0]
    print(f"Publishing assessed run: {run_source}")
    publication_summary_lf = utils.scan_parquet(run_source)

    publication_summary_lf = publication_summary_lf.filter(
        pl.col(IndCQC.main_job_role_clean_labelled)
        == PublishedJobGroupLabels.all_job_roles
    )

    annual_sampling_expr = publication_utils.reduced_data_filter_expr(
        today=date.today(),
        fy_start_month=FINANCIAL_YEAR_START_MONTH,
        lookback_fy_years=0,
        quarter_months=(FINANCIAL_YEAR_START_MONTH,),
    )

    publication_summary_lf = (
        publication_summary_lf.filter(annual_sampling_expr)
        .select(
            pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
            publication_utils.period_label_expr(),
            pl.col(IndCQC.current_region).alias(Pub.region),
            pl.col(IndCQC.primary_service_type)
            .cast(pl.Utf8)
            .replace(publication_utils.PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
            .alias(Pub.main_service),
            publication_utils.round_to_publication_bands(
                Pub.publication_filled_posts, Pub.estimated_filled_posts_rounded
            ),
            pl.col(Pub.publication_locationid_count).alias(Pub.cqc_locations),
            Pub.publication_filled_posts_april_perc_change,
            Pub.publication_filled_posts_cumulative_perc_change_short_term,
            Pub.publication_locationid_count_april_perc_change,
            Pub.publication_locationid_count_cumulative_perc_change_short_term,
        )
        .sort(publication_utils.DOWNLOAD_TABLE_SORT_COLUMNS)
    )

    utils.sink_to_parquet(
        lazy_df=publication_summary_lf,
        output_path=output_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--assessment_source",
            "S3 directory holding the assessed data of every run",
        ),
        (
            "--output_destination",
            "Destination s3 directory for the publication summary",
        ),
        (
            "--run_number",
            "Assessed run number to publish",
        ),
    )
    main(
        assessment_source=args.assessment_source,
        output_destination=args.output_destination,
        run_number=merge_utils.parse_run_number(args.run_number, required=True),
    )
