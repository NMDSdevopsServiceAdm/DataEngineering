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

    Nulls a location's/org's permanent, temporary, bank_or_pool, agency and
    other employment status counts where too few of its reported staff have a
    recorded permanent/temporary status to trust the split, recording why in
    employment_status_filtering_rule. Raw counts are kept untouched.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): destination for cleaned output
    """
    lf = utils.scan_parquet(merged_data_source)

    lf = cUtils.seed_employment_status_clean_columns(lf)
    lf = cUtils.null_employment_status_counts_where_org_permanent_temporary_ratio_is_too_low(
        lf
    )
    lf = cUtils.null_employment_status_counts_where_location_permanent_temporary_ratio_is_too_low(
        lf
    )

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
