from typing import Final

import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    nullify_job_role_count_when_source_not_ascwds,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

EXPANDED_ID: Final[str] = "expanded_id"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)


def main(
    merged_data_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): destination for output
    """
    print("Cleaning merged_ind_cqc dataset...")

    estimated_job_role_posts_lf = utils.scan_parquet(merged_data_source)
    print("Merged LazyFrame read in")

    estimated_job_role_posts_lf = nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    ).drop(
        IndCQC.estimate_filled_posts_source,
        IndCQC.ascwds_filled_posts_dedup_clean,
    )

    # TODO - Filter ASC-WDS worker data.

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_row_index(
        EXPANDED_ID
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=cleaned_data_destination,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for merged data",
        ),
        ("--cleaned_data_destination", "Destination s3 directory for cleaned data"),
    )
    main(
        merged_data_source=args.merged_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
