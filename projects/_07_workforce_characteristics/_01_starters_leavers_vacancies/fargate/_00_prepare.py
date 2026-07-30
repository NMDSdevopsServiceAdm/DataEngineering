import polars.selectors as cs

import polars_utils.cleaning_utils as cUtils
import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.cleaning_utils import apply_categorical_labels
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
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
        - reduce rows to quarterly import dates before two previous financial years
          and then earliest import day per month.
        - merge unpublished roles into 'other' groups
        - reshape the wide job-role columns into one row per job role, with
          employees/starters/leavers/vacancies metric columns

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    workplace_lf = (
        utils.scan_parquet(cleaned_ascwds_workplace_source)
        .filter(
            reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
        )
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWPClean.ascwds_workplace_import_date
            )
        )
    )

    workplace_lf = cUtils.merge_job_role_columns(
        workplace_lf, pUtils.unpublished_roles_mapping
    )

    # These columns refer to the toal for all job roles (28) and the total for job groups (29-32).
    # They are not required because we only want job roles at this stage.
    workplace_lf = workplace_lf.drop(
        cs.matches(pUtils.JOB_ROLE_SUMMARY_COLUMNS_PATTERN)
    )

    exploded_lf = pUtils.pivot_job_role_cols_to_rows(workplace_lf)

    # TODO: 1794 - Placeholder only.
    # exploded_lf = apply_categorical_labels()

    utils.sink_to_parquet(
        lazy_df=exploded_lf,
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
