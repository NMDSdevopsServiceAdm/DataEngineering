import tempfile
from datetime import date
from pathlib import Path

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    output_workbook_utils,
    publication_utils,
    workbook_helpers,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PublishedJobGroupLabels


def main(
    assessment_source: str,
    output_destination: str,
    run_number: int,
    workbook_destination: str,
    logo_source: str,
) -> None:
    """
    Writes the publication data for one assessed run: "All job roles" rows,
    one per year historically and monthly for the current financial year,
    with period, region, main service, rounded filled posts, location count
    and percentage change columns. Then builds the public data download
    workbook from that data and saves it to S3. A missing or empty logo fails
    the job.

    Args:
        assessment_source (str): s3 directory holding the assessed data of every run
        output_destination (str): destination s3 directory for the publication data
        run_number (int): the assessed run to publish
        workbook_destination (str): s3 directory for the data download workbook,
            named by archive_date and run_number
        logo_source (str): s3 uri of the PNG logo shown on the workbook's Frontpage

    Raises:
        ValueError: if the written publication data has no rows
    """
    run_source = merge_utils.resolve_run_sources([assessment_source], run_number)[0]
    print(f"Publishing assessed run: {run_source}")
    publication_summary_lf = utils.scan_parquet(run_source)

    publication_summary_lf = publication_summary_lf.filter(
        pl.col(IndCQC.main_job_role_clean_labelled)
        == PublishedJobGroupLabels.all_job_roles
    )

    # Historical rows are kept for April only (the financial year start month).
    annual_sampling_expr = publication_utils.reduced_data_filter_expr(
        today=date.today(),
        lookback_fy_years=0,
        quarter_months=(4,),
    )

    publication_output_lf = (
        publication_summary_lf.filter(annual_sampling_expr)
        .select(
            pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
            Pub.period_label_short,
            Pub.period_label_long,
            pl.col(IndCQC.current_region).alias(Pub.region),
            pl.col(IndCQC.primary_service_type)
            .cast(pl.Utf8)
            .replace(publication_utils.PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
            .alias(Pub.main_service),
            publication_utils.round_to_publication_bands(
                Pub.unfiltered_filled_posts, Pub.estimated_filled_posts_rounded
            ),
            pl.col(Pub.unfiltered_locationid_count).alias(Pub.cqc_locations),
            Pub.unfiltered_filled_posts_april_pct_change,
            Pub.unfiltered_short_sfc_cum_pct_change,
            Pub.unfiltered_locationid_count_april_pct_change,
            Pub.unfiltered_short_locationid_count_cum_pct_change,
        )
        .sort(publication_utils.DOWNLOAD_TABLE_SORT_KEYS)
    )

    utils.sink_to_parquet(
        lazy_df=publication_output_lf,
        output_path=output_destination,
    )

    # Read back the small written output so the plan above isn't computed twice.
    written_output_lf = utils.scan_parquet(output_destination)
    if written_output_lf.select(pl.len()).collect().item() == 0:
        raise ValueError(
            f"Run {run_number}: the publication data has no rows, so there is "
            "no data download to build"
        )
    logo = output_workbook_utils.get_logo_bytes(logo_source)
    workbook_name = output_workbook_utils.download_workbook_name(run_source)
    with tempfile.TemporaryDirectory() as workbook_folder:
        workbook_path = Path(workbook_folder) / workbook_name
        output_workbook_utils.build_download_workbook(
            written_output_lf, workbook_path, logo=logo
        )
        workbook_helpers.upload_to_s3(workbook_path, workbook_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--assessment_source",
            "S3 directory holding the assessed data of every run",
        ),
        (
            "--output_destination",
            "Destination s3 directory for the publication data",
        ),
        (
            "--run_number",
            "Assessed run number to publish",
        ),
        (
            "--workbook_destination",
            "Destination s3 directory for the data download workbook",
        ),
        (
            "--logo_source",
            "S3 uri of the PNG logo shown on the data download workbook's Frontpage",
        ),
    )
    main(
        assessment_source=args.assessment_source,
        output_destination=args.output_destination,
        run_number=merge_utils.parse_run_number(args.run_number, required=True),
        workbook_destination=args.workbook_destination,
        logo_source=args.logo_source,
    )
