import polars.selectors as cs

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
    not_null_filter_expr,
    reduced_data_filter_expr,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


def main(
    cleaned_ascwds_workplace_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS workplace dataset and then:
        - remove rows with a null location_id (ASCWDS includes non-CQC locations).
        - reduce rows to quarterly import dates before two previous financial years
          and then earliest import day per month.
        - merge unpublished roles into 'other' groups
        - relabel job role columns from jrNN codes to published labels
        - reshape job role columns into one row per job role

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    workplace_lf = (
        utils.scan_parquet(cleaned_ascwds_workplace_source)
        .filter(not_null_filter_expr(column=AWPClean.location_id))
        .filter(
            reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
        )
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWPClean.ascwds_workplace_import_date
            )
        )
    )

    # These columns refer to the total for all job roles (28) and the total for job groups (29-32).
    # They are not real ASC-WDS job role codes (not in MainJobRoleID), so they must be
    # dropped before reduce_to_published_roles runs.
    workplace_lf = workplace_lf.drop(cs.matches(r"^jr(28|29|30|31|32)"))

    workplace_lf = pUtils.reduce_to_published_roles(workplace_lf)
    workplace_lf = pUtils.relabel_job_role_columns(workplace_lf)
    workplace_job_role_lf = pUtils.reshape_job_role_cols_to_rows(workplace_lf)

    utils.sink_to_parquet(
        lazy_df=workplace_job_role_lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared data",
        ),
    )
    main(
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        prepared_data_destination=args.prepared_data_destination,
    )
