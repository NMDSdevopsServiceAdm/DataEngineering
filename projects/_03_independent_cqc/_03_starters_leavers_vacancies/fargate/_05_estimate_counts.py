from polars_utils import utils
from projects._03_independent_cqc._02_employment_status.fargate.utils.estimate_utils import (
    ESTIMATED_COUNT_COLUMNS,
    ESTIMATED_PERCENTAGE_COLUMNS,
)
from projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.estimate_counts_utils import (
    add_estimated_counts,
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
    employment_status_estimate_source: str,
    slv_estimate_source: str,
    estimated_data_destination: str,
) -> None:
    """Creates estimated counts of starters, leavers and vacancies.

    Estimated rates are joined to estimated employees to derive the counts.

    Args:
        employment_status_estimate_source (str): path to the employment
            status estimated data (headcount)
        slv_estimate_source (str): path to the SLV estimated rates
        estimated_data_destination (str): destination for output
    """
    employment_status_lf = utils.scan_parquet(
        employment_status_estimate_source,
        selected_columns=employment_status_estimate_columns,
    )
    slv_lf = utils.scan_parquet(
        slv_estimate_source, selected_columns=slv_estimate_columns
    )

    estimates_lf = employment_status_lf.join(slv_lf, on=JOIN_KEYS, how="left")

    estimates_lf = add_estimated_counts(estimates_lf)

    utils.sink_to_parquet(
        lazy_df=estimates_lf,
        output_path=estimated_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--employment_status_estimate_source",
            "Source s3 directory for employment status estimated data",
        ),
        (
            "--slv_estimate_source",
            "Source s3 directory for estimated SLV rates",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated count data",
        ),
    )
    main(
        employment_status_estimate_source=args.employment_status_estimate_source,
        slv_estimate_source=args.slv_estimate_source,
        estimated_data_destination=args.estimated_data_destination,
    )
