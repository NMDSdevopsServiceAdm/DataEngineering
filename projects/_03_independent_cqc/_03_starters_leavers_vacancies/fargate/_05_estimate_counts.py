import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._02_employment_status.fargate.utils.estimate_utils import (
    ESTIMATED_COUNT_COLUMNS,
    ESTIMATED_PERCENTAGE_COLUMNS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

JOIN_KEYS = [
    IndCQC.published_job_role_label,
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
]

employment_status_estimate_columns = [
    *JOIN_KEYS,
    *ESTIMATED_PERCENTAGE_COLUMNS,
    EmpStatus.estimated_percentage_source,
    *ESTIMATED_COUNT_COLUMNS,
    EmpStatus.estimated_employees,
]

slv_estimate_columns = [
    *JOIN_KEYS,
    SLVCols.estimated_starter_rate,
    SLVCols.estimated_turnover_rate,
    SLVCols.estimated_vacancy_rate,
    SLVCols.estimated_starter_rate_source,
    SLVCols.estimated_turnover_rate_source,
    SLVCols.estimated_vacancy_rate_source,
]


def main(
    slv_estimate_source: str,
    employment_status_estimate_source: str,
    estimated_data_destination: str,
) -> None:
    """Creates estimated counts of starters, leavers and vacancies.

    Estimated rates are joined to estimated employees, then:
        starters = employees * starter rate
        leavers = employees * turnover rate
        vacancies = (employees * vacancy rate) / (1 - vacancy rate)

    Args:
        slv_estimate_source (str): path to the SLV estimated rates
        employment_status_estimate_source (str): path to the employment
            status estimated data (headcount)
        estimated_data_destination (str): destination for output
    """
    employment_status_estimate_lf = utils.scan_parquet(
        employment_status_estimate_source,
        selected_columns=employment_status_estimate_columns,
    )
    slv_estimate_lf = utils.scan_parquet(
        slv_estimate_source, selected_columns=slv_estimate_columns
    )

    slv_estimate_lf = slv_estimate_lf.join(
        employment_status_estimate_lf, on=JOIN_KEYS, how="left"
    )

    employees = pl.col(EmpStatus.estimated_employees)
    vacancy_rate = pl.col(SLVCols.estimated_vacancy_rate)
    slv_estimate_lf = slv_estimate_lf.with_columns(
        (employees * pl.col(SLVCols.estimated_starter_rate)).alias(
            SLVCols.estimated_starters
        ),
        (employees * pl.col(SLVCols.estimated_turnover_rate)).alias(
            SLVCols.estimated_leavers
        ),
        ((employees * vacancy_rate) / (1 - vacancy_rate)).alias(
            SLVCols.estimated_vacancies
        ),
    )

    utils.sink_to_parquet(
        lazy_df=slv_estimate_lf,
        output_path=estimated_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--slv_estimate_source",
            "Source s3 directory for estimated SLV rates",
        ),
        (
            "--employment_status_estimate_source",
            "Source s3 directory for employment status estimated data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated count data",
        ),
    )
    main(
        slv_estimate_source=args.slv_estimate_source,
        employment_status_estimate_source=args.employment_status_estimate_source,
        estimated_data_destination=args.estimated_data_destination,
    )
