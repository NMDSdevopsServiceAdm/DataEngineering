"""THROWAWAY. Instrumented copy of _01_merge.py for the 2102 monthly-data spike.

Mirrors _01_merge.main exactly, with a RunDiagnostics checkpoint either side of each
step, so the RSS curve can be attributed to a stage rather than to the job as a whole.
The open question is whether retaining all historical data at monthly resolution
(instead of quarterly-sampling anything older than the last 2 financial years) pushes
peak memory near the task's ceiling.

Writes to its own dataset names so it can never overwrite the real pipeline's output.
Delete this file, its Dockerfile COPY line, its terraform module and its step function
definition once the investigation concludes.
"""

import polars as pl

from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from polars_utils.run_diagnostics import RunDiagnostics
from polars_utils.utils import split_s3_uri
from projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate.utils.merge_utils import (
    join_estimates_to_ascwds,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

SAMPLE_INTERVAL_SECONDS: float = 10

metadata_columns = {
    IndCQC.name: str,
    IndCQC.provider_id: CatColType.ProviderCatType,
    IndCQC.brand_id: CatColType.BrandCatType,
    IndCQC.services_offered: pl.List(str),
    IndCQC.primary_service_type_second_level: pl.Categorical,
    IndCQC.care_home: CatColType.CareHomeEnumType,
    IndCQC.dormancy: pl.Categorical,
    IndCQC.number_of_beds: pl.Int16,
    IndCQC.imputed_registration_date: pl.Date,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.establishment_id: CatColType.EstablishmentCatType,
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
    IndCQC.estimate_filled_posts_source: CatColType.EstimatesFilledPostSourceEnumType,
    IndCQC.ascwds_filled_posts_source: CatColType.AscwdsFilledPostsSourceEnumType,
    IndCQC.care_home_model: pl.Float32,
    IndCQC.imputed_pir_filled_posts_model: pl.Float32,
    IndCQC.imputed_posts_care_home_model: pl.Float32,
    IndCQC.imputed_posts_non_res_combined_model: pl.Float32,
    IndCQC.non_res_combined_model: pl.Float32,
    IndCQC.pir_people_directly_employed_dedup: pl.Int64,
    IndCQC.posts_rolling_average_model: pl.Float32,
    IndCQC.ct_care_home_total_employed_imputed: pl.Float32,
    IndCQC.ct_non_res_care_workers_employed_imputed: pl.Float32,
    IndCQC.care_home_status_count: pl.Int16,
}
ascwds_columns_to_import = {
    IndCQC.ascwds_worker_import_date: pl.Date,
    IndCQC.establishment_id: CatColType.EstablishmentCatType,
    IndCQC.main_job_role_clean_labelled: CatColType.JobRoleCatType,
    IndCQC.ascwds_job_role_counts: pl.Int16,
}
transformation_columns = {
    IndCQC.location_id: CatColType.LocationCatType,
    IndCQC.cqc_location_import_date: pl.Date,
    IndCQC.establishment_id: CatColType.EstablishmentCatType,
    IndCQC.ascwds_workplace_import_date: pl.Date,
    IndCQC.estimate_filled_posts: pl.Float32,
    IndCQC.estimate_filled_posts_source: CatColType.EstimatesFilledPostSourceEnumType,
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
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
    Instrumented copy of the job role estimates merge step.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        merged_data_destination (str): destination for merged output
        metadata_destination (str): destination for metadata
    """
    data_bucket, _ = split_s3_uri(merged_data_destination)
    diagnostics = RunDiagnostics(
        "job_role_estimates_01_merge_prototype",
        data_bucket,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        combined_schema = transformation_columns | metadata_columns
        full_estimates_lf = (
            utils.scan_parquet(estimates_source)
            .select(list(combined_schema))
            .with_row_index(name=IndCQC.id_per_locationid_import_date)
            .with_columns(utils.cast_to_schema(combined_schema))
        )
        diagnostics.checkpoint("after_select_and_cast", full_estimates_lf)

        estimated_posts_base_lf = full_estimates_lf.select(
            IndCQC.id_per_locationid_import_date, *list(transformation_columns)
        )
        # This will be joined on at the end.
        metadata_lf = full_estimates_lf.select(
            IndCQC.id_per_locationid_import_date, *list(metadata_columns)
        )

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
        diagnostics.checkpoint("after_join", estimated_job_role_posts_lf)

        utils.sink_to_parquet(
            lazy_df=estimated_job_role_posts_lf,
            output_path=merged_data_destination,
        )
        diagnostics.checkpoint("after_sink_merged")

        utils.sink_to_parquet(
            lazy_df=metadata_lf,
            output_path=metadata_destination,
        )
        diagnostics.checkpoint("after_sink_metadata")
    finally:
        diagnostics.stop()


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
