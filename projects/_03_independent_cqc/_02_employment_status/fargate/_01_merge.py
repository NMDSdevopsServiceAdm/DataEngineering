import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.merge_utils as mUtils
import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as pWorkerUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.employment_status_rates_columns import (
    EmploymentStatusRatesColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

worker_columns = [
    AWKClean.location_id,
    AWKClean.establishment_id,
    AWKClean.ascwds_worker_import_date,
    IndCQC.published_job_role_label,
    *pWorkerUtils.EMPLOYMENT_STATUS_LABEL_TO_COLUMN.values(),
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
    prepared_worker_source: str,
    employment_status_rates_source: str,
    merged_data_destination: str,
) -> None:
    """
    Merges job role estimates with metadata and prepared worker employment status data.

    Args:
        metadata_source (str): path to the estimates ind cqc filled posts data
        job_role_estimates_source (str): path to the job role estimates data
        prepared_worker_source (str): path to the prepared ascwds worker employment
            status data
        employment_status_rates_source (str): path to the employment status rates csv
        merged_data_destination (str): destination for merged output
    """

    metadata_lf = utils.scan_parquet(
        source=metadata_source, selected_columns=metadata_columns
    )
    job_role_estimates_lf = utils.scan_parquet(
        source=job_role_estimates_source, selected_columns=job_role_estimates_columns
    )

    job_role_estimates_lf = mUtils.collapse_job_role_estimates_to_published_labels(
        job_role_estimates_lf
    )

    job_role_estimates_lf = job_role_estimates_lf.join(
        metadata_lf,
        on=IndCQC.id_per_locationid_import_date,
        how="left",
    )

    # The source CSV is expected to already be trimmed to exactly these columns, in this
    # order, and to only the current weighting year's rows — scan_csv's schema is matched
    # positionally, not by name, so a reordered file would silently load into the wrong
    # columns with no error.
    employment_status_rates_schema = pl.Schema(
        [
            (EmpStatRates.service, pl.Categorical()),
            (EmpStatRates.weighting_job_role, pl.Categorical()),
            (EmpStatRates.emp_stat_perm, pl.Float32),
            (EmpStatRates.emp_stat_temp, pl.Float32),
            (EmpStatRates.emp_stat_bank_or_pool, pl.Float32),
            (EmpStatRates.emp_stat_agency, pl.Float32),
            (EmpStatRates.emp_stat_other, pl.Float32),
        ]
    )

    employment_status_rates_lf = pl.scan_csv(
        employment_status_rates_source, schema=employment_status_rates_schema
    )

    job_role_estimates_lf = mUtils.apply_employment_status_magic_numbers(
        job_role_estimates_lf, employment_status_rates_lf
    )

    worker_lf = utils.scan_parquet(
        prepared_worker_source, selected_columns=worker_columns
    )

    # ascwds_workplace_import_date comes from metadata_lf (joined above), not from a
    # prepared workplace dataset - this stage never touches workplace/SLV data at all.
    job_role_estimates_lf = job_role_estimates_lf.join(
        worker_lf,
        left_on=[
            IndCQC.location_id,
            IndCQC.establishment_id,
            IndCQC.ascwds_workplace_import_date,
            IndCQC.published_job_role_label,
        ],
        right_on=[
            AWKClean.location_id,
            AWKClean.establishment_id,
            AWKClean.ascwds_worker_import_date,
            IndCQC.published_job_role_label,
        ],
        how="left",
    )

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
            "--prepared_worker_source",
            "Source s3 directory for prepared ascwds worker employment status data",
        ),
        (
            "--employment_status_rates_source",
            "Source s3 directory for employment status rates data",
        ),
        (
            "--merged_data_destination",
            "Destination s3 directory for merged data",
        ),
    )
    main(
        metadata_source=args.metadata_source,
        job_role_estimates_source=args.job_role_estimates_source,
        prepared_worker_source=args.prepared_worker_source,
        employment_status_rates_source=args.employment_status_rates_source,
        merged_data_destination=args.merged_data_destination,
    )
