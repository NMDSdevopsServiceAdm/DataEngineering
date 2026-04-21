import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.merge_utils import (
    join_estimates_to_ascwds,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

EstablishmentCatType = pl.Categorical(
    pl.Categories("establishment", namespace="filled_posts")
)
LocationCatType = pl.Categorical(pl.Categories("location", namespace="filled_posts"))
JobRoleEnumType = pl.Enum(AscwdsWorkerValueLabelsJobGroup.all_roles())
EstimatesFilledPostSourceEnumType = pl.Enum(
    [
        IndCQC.imputed_pir_filled_posts_model,
        IndCQC.ascwds_pir_merged,
        IndCQC.imputed_posts_care_home_model,
        IndCQC.care_home_model,
        IndCQC.imputed_posts_non_res_combined_model,
        IndCQC.non_res_combined_model,
        IndCQC.posts_rolling_average_model,
    ]
)
PrimaryServiceEnumType = pl.Enum(
    [
        PrimaryServiceType.care_home_only,
        PrimaryServiceType.care_home_with_nursing,
        PrimaryServiceType.non_residential,
    ]
)

metadata_columns = {
    IndCQC.name: str,
    IndCQC.provider_id: str,
    IndCQC.services_offered: pl.List(str),
    IndCQC.primary_service_type_second_level: pl.Categorical,
    IndCQC.care_home: pl.Categorical,
    IndCQC.dormancy: pl.Categorical,
    IndCQC.number_of_beds: pl.Int16,
    IndCQC.imputed_registration_date: pl.Date,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.organisation_id: str,
    IndCQC.worker_records_bounded: pl.Int16,
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
    IndCQC.ascwds_pir_merged: pl.Float32,
    IndCQC.ascwds_filtering_rule: pl.Categorical,
    IndCQC.current_ons_import_date: pl.Date,
    IndCQC.current_cssr: pl.Categorical,
    IndCQC.current_region: pl.Categorical,
    IndCQC.current_icb: pl.Categorical,
    IndCQC.current_rural_urban_indicator_2011: pl.Categorical,
    IndCQC.current_lsoa21: pl.Categorical,
    IndCQC.current_msoa21: pl.Categorical,
    IndCQC.estimate_filled_posts_source: EstimatesFilledPostSourceEnumType,
}
ascwds_columns_to_import = {
    IndCQC.ascwds_worker_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.main_job_role_clean_labelled: JobRoleEnumType,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}
transformation_columns = {
    IndCQC.location_id: LocationCatType,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.establishment_id: EstablishmentCatType,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.estimate_filled_posts_source: EstimatesFilledPostSourceEnumType,
    IndCQC.primary_service_type: PrimaryServiceEnumType,
    IndCQC.registered_manager_names: pl.List(str),
    IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
}


def main(
    estimates_source: str,
    ascwds_job_role_counts_source: str,
    merged_data_destination: str,
    metadata_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        merged_data_destination (str): destination for merged output
        metadata_destination (str): destination for metadata
    """
    combined_schema = transformation_columns | metadata_columns
    full_estimates_lf = (
        utils.scan_parquet(estimates_source)
        .select(list(combined_schema))
        .with_row_index(name=IndCQC.row_id)
        .with_columns(utils.cast_to_schema(combined_schema))
    )
    estimated_posts_base_lf = full_estimates_lf.select(
        IndCQC.row_id, *list(transformation_columns)
    )
    # This will be joined on at the end.
    metadata_lf = full_estimates_lf.select(IndCQC.row_id, *list(metadata_columns))

    col_name_map = {
        IndCQC.ascwds_worker_import_date: IndCQC.ascwds_workplace_import_date
    }
    ascwds_job_role_counts_lf = (
        utils.scan_parquet(ascwds_job_role_counts_source)
        .select(list(ascwds_columns_to_import))
        .with_columns(utils.cast_to_schema(ascwds_columns_to_import))
        .rename(col_name_map)
    )

    estimated_job_role_posts_lf = join_estimates_to_ascwds(
        estimated_posts_base_lf,
        ascwds_job_role_counts_lf,
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=merged_data_destination,
        append=False,
    )

    utils.sink_to_parquet(
        lazy_df=metadata_lf,
        output_path=metadata_destination,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--ascwds_job_role_counts_source",
            "Source s3 directory for parquet ASCWDS worker job role counts dataset",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
        ("--metadata_destination", "Destination s3 directory for metadata"),
    )
    main(
        estimates_source=args.estimates_source,
        ascwds_job_role_counts_source=args.ascwds_job_role_counts_source,
        merged_data_destination=args.merged_data_destination,
        metadata_destination=args.metadata_destination,
    )
