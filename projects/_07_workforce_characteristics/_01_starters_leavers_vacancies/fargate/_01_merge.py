import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as mUtils
from polars_utils import expressions as expr
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.employee_status_rates_columns import (
    EmployeeStatusRatesColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

workplace_columns = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
]

metadata_columns = [
    IndCQC.id_per_locationid_import_date,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.brand_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_registration_date,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.estimate_filled_posts_source,
]

job_role_estimates_columns = [
    IndCQC.id_per_locationid_import_date_job_role,
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.primary_service_type,
    IndCQC.id_per_locationid_import_date,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
    IndCQC.main_job_group_labelled,
]


def main(
    metadata_source: str,
    job_role_estimates_source: str,
    prepared_slv_dataset_source: str,
    employee_status_rates_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges estimates of filled posts data with AWS-WDS data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        prepared_slv_dataset_source (str): path to the cleaned ascwds workplace data
        employee_status_rates_source (str): path to the employee status rates csv
        merged_data_destination (str): destination for merged output
    """

    metadata_lf = utils.scan_parquet(
        source=metadata_source, selected_columns=metadata_columns
    )
    job_role_estimates_lf = utils.scan_parquet(
        source=job_role_estimates_source, selected_columns=job_role_estimates_columns
    )
    cleaned_ascwds_workplace_lf = utils.scan_parquet(
        prepared_slv_dataset_source
    ).select(
        *[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        expr.is_slv_job_role_column()
    )

    target_weighting_year = "2025/26"

    employee_status_rates_schema = pl.Schema(
        [
            (EmpStatRates.service, pl.Categorical()),
            (EmpStatRates.weighting_year, pl.Categorical()),
            (EmpStatRates.weighting_job_role, pl.Categorical()),
            (EmpStatRates.permanent, pl.String),
            (EmpStatRates.temporary, pl.String),
            (EmpStatRates.bank_or_pool, pl.String),
            (EmpStatRates.agency, pl.String),
            (EmpStatRates.other, pl.String),
            (EmpStatRates.filled_posts, pl.String),
            (EmpStatRates.weighting_date, pl.String),
            (EmpStatRates.emp_stat_perm, pl.Float32),
            (EmpStatRates.emp_stat_temp, pl.Float32),
            (EmpStatRates.emp_stat_bank_or_pool, pl.Float32),
            (EmpStatRates.emp_stat_agency, pl.Float32),
            (EmpStatRates.emp_stat_other, pl.Float32),
        ]
    )
    employee_status_rates_output_columns = [
        EmpStatRates.service,
        EmpStatRates.weighting_year,
        EmpStatRates.weighting_job_role,
        EmpStatRates.emp_stat_perm,
        EmpStatRates.emp_stat_temp,
        EmpStatRates.emp_stat_bank_or_pool,
        EmpStatRates.emp_stat_agency,
        EmpStatRates.emp_stat_other,
    ]

    employee_status_rates_lf = (
        pl.scan_csv(employee_status_rates_source, schema=employee_status_rates_schema)
        .select(employee_status_rates_output_columns)
        .filter(~pl.all_horizontal(pl.all().is_null()))
        .filter(pl.col(EmpStatRates.weighting_year) == target_weighting_year)
        .drop(EmpStatRates.weighting_year)
    )

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
            "--prepared_slv_dataset_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--employee_status_rates_source",
            "Source s3 directory for employee status rates data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        metadata_source=args.metadata_source,
        job_role_estimates_source=args.job_role_estimates_source,
        prepared_slv_dataset_source=args.prepared_slv_dataset_source,
        employee_status_rates_source=args.employee_status_rates_source,
        merged_data_destination=args.merged_data_destination,
    )
