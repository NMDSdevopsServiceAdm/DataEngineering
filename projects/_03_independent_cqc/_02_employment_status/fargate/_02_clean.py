from projects._03_independent_cqc._02_employment_status.fargate.utils import (
    clean_utils as cUtils,
)
from polars_utils import utils


def main(
    merged_data_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Cleans the merged employment status data.

    Deduplicates the 5 employment status count columns as a single unit and
    adds a percentage-share column per employment status. Then nulls a
    location's/org's counts where too few of its staff have a recorded
    permanent/temporary status, recording why in
    employment_status_filtering_rule.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): destination for cleaned output
    """
    lf = utils.scan_parquet(merged_data_source)

    lf = cUtils.create_employment_status_percentage_columns(lf)
    lf = cUtils.null_counts_for_low_org_ratio(lf)
    lf = cUtils.null_counts_for_low_location_ratio(lf)

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--cleaned_data_destination",
            "Destination s3 directory for cleaned data",
        ),
    )
    main(
        merged_data_source=args.merged_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
