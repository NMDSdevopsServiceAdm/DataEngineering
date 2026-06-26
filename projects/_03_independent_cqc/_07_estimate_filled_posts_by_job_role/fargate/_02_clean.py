import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.clean_utils as cUtils
from polars_utils import utils
from polars_utils.filtering_utils import (
    add_filtering_rule_column,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes as CatColType,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    add_job_role_groups_column,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import JobRoleFilteringRule

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

estimates_by_job_role_schema = {
    IndCQC.id_per_locationid_import_date: pl.UInt32,
    IndCQC.location_id: CatColType.LocationCatType,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.estimate_filled_posts_source: CatColType.EstimatesFilledPostSourceEnumType,
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
    IndCQC.main_job_role_clean_labelled: CatColType.JobRoleEnumType,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}

metadata_columns_to_import_schema = {
    IndCQC.id_per_locationid_import_date: pl.UInt32,
    IndCQC.provider_id: CatColType.ProviderCatType,
    IndCQC.brand_id: CatColType.BrandCatType,
}


def main(
    merged_data_source: str,
    merged_metadata_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        merged_data_source (str): path to the merged data
        merged_metadata_source (str): path to the merged metadata
        cleaned_data_destination (str): destination for output
    """
    print("Cleaning merged_ind_cqc dataset...")

    estimated_job_role_posts_lf = utils.scan_parquet(
        merged_data_source,
        schema=estimates_by_job_role_schema,
        selected_columns=estimates_by_job_role_schema.keys(),
    )
    print("Merged LazyFrame read in")

    merged_metadata_lf = utils.scan_parquet(
        merged_metadata_source,
        schema=metadata_columns_to_import_schema,
        selected_columns=metadata_columns_to_import_schema.keys(),
    )
    print("Merged Metadata LazyFrame read in")

    # add metadata columns to estimated_job_role_posts_lf
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        merged_metadata_lf,
        on=IndCQC.id_per_locationid_import_date,
        how="left",
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_row_index(
        IndCQC.id_per_locationid_import_date_job_role
    )

    estimated_job_role_posts_lf = cUtils.nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    ).drop(
        IndCQC.estimate_filled_posts_source,
        IndCQC.ascwds_filled_posts_dedup_clean,
    )

    estimated_job_role_posts_lf = add_job_role_groups_column(
        estimated_job_role_posts_lf, IndCQC.main_job_group_labelled
    )

    estimated_job_role_posts_lf = add_filtering_rule_column(
        estimated_job_role_posts_lf,
        filter_rule_col_name=IndCQC.job_role_filtering_rule,
        col_to_filter=IndCQC.ascwds_job_role_counts,
        populated_rule=JobRoleFilteringRule.populated,
        missing_rule=JobRoleFilteringRule.missing_raw_data,
        categorical_type=CatColType.JobRoleFilteringRuleCatType,
    )

    estimated_job_role_posts_lf = cUtils.filter_job_role_group_equal_zero(
        estimated_job_role_posts_lf
    )

    # estimated_job_role_posts_lf = cUtils.filter_job_role_group_outliers(
    #     estimated_job_role_posts_lf, id_column=IndCQC.brand_id
    # )

    estimated_job_role_posts_lf = cUtils.filter_job_role_group_outliers(
        estimated_job_role_posts_lf, id_column=IndCQC.provider_id
    )

    estimated_job_role_posts_lf = cUtils.filter_job_role_group_outliers(
        estimated_job_role_posts_lf, id_column=IndCQC.location_id
    )

    # Drop job role group as this will be reallocated in later job
    # Drop providerid and brandid which are just used for filtering and not needed for impute and estimate steps.
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.drop(
        IndCQC.main_job_group_labelled,
        IndCQC.provider_id,
        IndCQC.brand_id,
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--merged_metadata_source",
            "Source s3 directory for merged metadata",
        ),
        ("--cleaned_data_destination", "Destination s3 directory for cleaned data"),
    )
    main(
        merged_data_source=args.merged_data_source,
        merged_metadata_source=args.merged_metadata_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
