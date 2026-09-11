from datetime import date

import polars as pl

from polars_utils import utils
from polars_utils.filtering_utils import reduced_data_filter_expr
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

    The remaining capacity tracker filters, the aggregation and percentage
    change columns are currently placeholders and don't yet apply any real
    filtering.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): destination s3 directory for the cleaned data
    """
    merged_lf = utils.scan_parquet(merge_data_source)

    # Publication policy: full retention for 2 financial years, quarterly
    # sampling further back, nothing before 6 financial years ago.
    today = date.today()
    fy_year = today.year if today.month >= 4 else today.year - 1
    cutoff_date = date(fy_year - 6, 4, 1)
    cleaned_lf = merged_lf.filter(reduced_data_filter_expr(cutoff_date=cutoff_date))

    cleaned_lf = cleaned_lf.with_columns(
        (pl.col(IndCQC.care_home_status_count) == 1).alias(Pub.consistent_service)
    )

    # See clean_utils/test_clean_utils for placeholders.

    cleaned_lf = cleaned_lf.with_columns(
        pl.coalesce(
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        ).alias(Pub.ct_total_employed_imputed)
    )

    long_term_from_date = date(fy_year - 5, 7, 1)
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

    # TODO: Add remaining capacity tracker filters (consistent service, dispersion).

    # TODO: Aggregate on job role, primary_service_type and current_region.

    # TODO: Add rows for 'England', 'All CQC locations' and 'All CQC care homes'.

    # TODO: Add percentage change between rows.

    # TODO: Add cumulative percentage change from given start period.

    utils.sink_to_parquet(
        lazy_df=cleaned_lf,
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
