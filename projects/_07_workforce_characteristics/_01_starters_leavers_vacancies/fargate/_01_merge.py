import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as mUtils
from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes as CatColType,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

metadata_columns_schema = {
    IndCQC.id_per_locationid_import_date: pl.Int64,
    IndCQC.name: str,
    IndCQC.provider_id: CatColType.ProviderCatType,
    IndCQC.brand_id: CatColType.BrandCatType,
    IndCQC.services_offered: pl.List(str),
    IndCQC.primary_service_type_second_level: pl.Categorical,
    IndCQC.care_home: pl.Categorical,
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
    IndCQC.estimate_filled_posts_source: CatColType.EstimatesFilledPostSourceEnumType,
}


def main(
    metadata_source: str,
    job_role_estimates_source: str,
    cleaned_ascwds_workplace_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        merged_data_destination (str): destination for merged output
    """
    # TODO: Placeholder only
    # mUtils.create_list_of_cols_for_ascwds()

    metadata_lf = utils.scan_parquet(
        source=metadata_source, schema=metadata_columns_schema
    )
    job_role_estimates_lf = utils.scan_parquet(job_role_estimates_source)
    cleaned_ascwds_workplace_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)

    # TODO: Placeholder only
    # mUtils.convert_ascwds_job_role_columns_to_rows()

    # TODO: Placeholder only
    # mUtils.join_datasets()

    # TODO: Placeholder only
    # mUtils.apply_employment_status_magic_numbers()

    utils.sink_to_parquet(
        lazy_df=job_role_estimates_lf,
        output_path=merged_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--metadata_source",
            "Source s3 directory for metadata",
        ),
        (
            "--job_role_estimates_source",
            "Source s3 directory for job role estimates data",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        metadata_source=args.metadata_source,
        job_role_estimates_source=args.job_role_estimates_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        merged_data_destination=args.merged_data_destination,
    )
