from polars_utils import utils
from projects._08_publication._01_job_role_estimates.fargate.utils import (
    clean_utils as cUtils,
)


def main(
    merge_data_source: str,
    clean_destination: str,
) -> None:
    """
    Cleans merged job role data.

    The capacity tracker filters, the aggregation and percentage change columns
    are currently placeholders and don't yet apply any real filtering.

    Args:
        merge_data_source (str): source s3 directory for merged data
        clean_destination (str): destination s3 directory for the cleaned data
    """
    lf = utils.scan_parquet(merge_data_source)

    # See clean_utils/test_clean_utils for placeholders.

    # TODO: Add capacity tracker filters. Call one at a time as three are developed.

    # TODO: Aggregate on job role, primary_service_type and current_region.

    # TODO: Add rows for 'England', 'All CQC locations' and 'All CQC care homes'.

    # TODO: Add percentage change between rows.

    # TODO: Add cumulative percentage change from given start period.

    utils.sink_to_parquet(
        lazy_df=lf,
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
