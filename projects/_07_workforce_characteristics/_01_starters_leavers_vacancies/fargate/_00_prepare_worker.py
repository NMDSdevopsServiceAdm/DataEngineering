import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_worker_utils as pWorkerUtils
from polars_utils import utils
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
    not_null_filter_expr,
    reduced_data_filter_expr,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def main(
    cleaned_ascwds_worker_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS worker dataset and prepare it for employment
    status estimation.

    Drops rows with a null location_id (ASCWDS includes non-CQC locations) and
    reduces import dates to the same quarterly-beyond-2-years/earliest-file-
    per-month grain _00_prepare_workplace uses, so the output's import dates
    line up with job_role_estimates when the merge step joins them.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ascwds worker data
        prepared_data_destination (str): destination for output
    """
    worker_lf = (
        utils.scan_parquet(cleaned_ascwds_worker_source)
        .filter(not_null_filter_expr(column=AWKClean.location_id))
        .filter(reduced_data_filter_expr(date_col=AWKClean.ascwds_worker_import_date))
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWKClean.ascwds_worker_import_date
            )
        )
    )
    worker_lf = pWorkerUtils.collapse_job_roles_to_published_labels(worker_lf)

    employment_status_summary_lf = pWorkerUtils.aggregate_employment_status_data(
        worker_lf
    )
    employment_status_summary_lf = pWorkerUtils.reshape_employment_status_data(
        employment_status_summary_lf
    )

    utils.sink_to_parquet(
        lazy_df=employment_status_summary_lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared worker data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        prepared_data_destination=args.prepared_data_destination,
    )
