from datetime import date

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    publication_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub


def main(
    prep_source: str,
    output_destination: str,
    t0_destination: str,
    t1_destination: str,
    t2_destination: str,
    run_number: int,
) -> None:
    """
    Builds the public data for one selected, prepared run: the publication
    summary by import date, job role, region and service type, and the
    T0/T1/T2 data-download tables.

    Args:
        prep_source (str): s3 directory holding the prepared data of every run
        output_destination (str): destination s3 directory for the publication summary
        t0_destination (str): destination s3 directory for the T0 estimates
            download table
        t1_destination (str): destination s3 directory for the T1 filled
            posts percentage change download table
        t2_destination (str): destination s3 directory for the T2 location
            count percentage change download table
        run_number (int): the prepared run to publish
    """
    run_source = merge_utils.resolve_run_sources([prep_source], run_number)[0]
    print(f"Publishing prepared run: {run_source}")
    prepared_lf = utils.scan_parquet(run_source)

    publication_summary_lf = publication_utils.aggregate_to_publication_rows(
        prepared_lf, include_assessment=False
    )
    publication_summary_lf = publication_utils.add_rows_for_publication_groups(
        prepared_lf, publication_summary_lf, include_assessment=False
    )

    publication_summary_lf = publication_summary_lf.with_columns(
        publication_utils.format_large_number(
            Pub.publication_filled_posts, Pub.publication_filled_posts_formatted
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
        output_path=output_destination,
    )

    today = date.today()

    t0_estimates_lf = publication_utils.build_t0_estimates_download_table(
        publication_summary_lf, today=today
    )
    utils.sink_to_parquet(
        lazy_df=t0_estimates_lf,
        output_path=t0_destination,
    )

    t1_filled_posts_perc_change_lf = (
        publication_utils.build_t1_filled_posts_perc_change_download_table(
            publication_summary_lf, today=today
        )
    )
    utils.sink_to_parquet(
        lazy_df=t1_filled_posts_perc_change_lf,
        output_path=t1_destination,
    )

    t2_location_count_perc_change_lf = (
        publication_utils.build_t2_location_count_perc_change_download_table(
            publication_summary_lf, today=today
        )
    )
    utils.sink_to_parquet(
        lazy_df=t2_location_count_perc_change_lf,
        output_path=t2_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--prep_source",
            "S3 directory holding the prepared data of every run",
        ),
        (
            "--output_destination",
            "Destination s3 directory for the publication summary",
        ),
        (
            "--t0_destination",
            "Destination s3 directory for the T0 estimates download table",
        ),
        (
            "--t1_destination",
            "Destination s3 directory for the T1 filled posts percentage "
            "change download table",
        ),
        (
            "--t2_destination",
            "Destination s3 directory for the T2 location count percentage "
            "change download table",
        ),
        (
            "--run_number",
            "Prepared run number to publish",
        ),
    )
    main(
        prep_source=args.prep_source,
        output_destination=args.output_destination,
        t0_destination=args.t0_destination,
        t1_destination=args.t1_destination,
        t2_destination=args.t2_destination,
        run_number=merge_utils.parse_required_run_number(args.run_number),
    )
