import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.bakeoff_utils as bUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

# Deliberately narrow. The source is one row per location, import date and job role, so pruning
# to only what the bake-off needs is the main defence against running out of memory during the
# per location and job role window functions.
bakeoff_source_schema = {
    IndCQC.location_id: CatColType.LocationCatType,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
    IndCQC.main_job_role_clean_labelled: CatColType.JobRoleEnumType,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}


def main(
    cleaned_data_source: str,
    bakeoff_destination: str,
    bakeoff_by_service_destination: str,
) -> None:
    """
    Compares upfront fill regimes for the ASC-WDS job role rolling ratio.

    Investigation job for ticket 1859. Builds the rolling ratio each candidate regime would
    produce, alongside diagnostics describing how much real data sits behind it, so the
    alternatives can be charted before one is adopted as the trendline for extrapolation.

    The pre-aggregate is collected once because it is needed at two stratifications; it is only
    a few tens of thousands of rows by that point, and collecting avoids running the expensive
    location level window functions twice.

    Args:
        cleaned_data_source (str): path to the cleaned job role data
        bakeoff_destination (str): destination for the output split by size group
        bakeoff_by_service_destination (str): destination for the output without size group
    """

    print("Building job role rolling ratio bake-off...")

    job_role_lf = utils.scan_parquet(cleaned_data_source, schema=bakeoff_source_schema)
    print("Cleaned LazyFrame read in")

    job_role_lf = bUtils.prepare_variants(job_role_lf)

    pre_aggregate_lf = (
        bUtils.build_pre_aggregate(job_role_lf).collect(engine="streaming").lazy()
    )
    print("Pre-aggregate collected")

    utils.sink_to_parquet(
        lazy_df=bUtils.build_bakeoff(
            pre_aggregate_lf,
            group_cols=[
                IndCQC.primary_service_type,
                IndCQC.estimate_filled_posts_size_group,
            ],
        ),
        output_path=bakeoff_destination,
    )

    utils.sink_to_parquet(
        lazy_df=bUtils.build_bakeoff(
            bUtils.collapse_size_groups(pre_aggregate_lf),
            group_cols=[IndCQC.primary_service_type],
        ),
        output_path=bakeoff_by_service_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for cleaned job role data",
        ),
        (
            "--bakeoff_destination",
            "Destination s3 directory for the bake-off split by size group",
        ),
        (
            "--bakeoff_by_service_destination",
            "Destination s3 directory for the bake-off without size group",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        bakeoff_destination=args.bakeoff_destination,
        bakeoff_by_service_destination=args.bakeoff_by_service_destination,
    )
