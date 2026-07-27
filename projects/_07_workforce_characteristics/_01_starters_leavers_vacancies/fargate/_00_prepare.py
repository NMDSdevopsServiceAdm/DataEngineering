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
    """Load the cleaned ASCWDS workplace dataset, reduce it, and save it.

    Rows are reduced to the same retention window the downstream job role estimates
    already use, so the two datasets line up at the merge step, and further reduced to
    one file per calendar month to match that dataset's monthly granularity. The retention
    filter is applied first even though the two reductions are order-independent (retention
    keeps or drops whole months, never partial ones): it's a cheap date-range predicate that
    lets most of the older history get discarded before the more expensive per-month-min
    window used by the monthly reduction has to run over it. Both filters are attached
    directly to the scan so the predicates are pushed down to the parquet source rather
    than running over a materialised frame - this dataset is both long and wide, so
    reading it in full before filtering is what we are avoiding.

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

    # TODO: 1796 - Placeholder only.
    # pUtils.reduce_to_published_roles()

    # TODO: Backlog ticket/no number - Placeholder only.
    # pUtils.pivot_job_role_cols_to_rows()

    # TODO: 1795 - Placeholder only.
    # pUtils.convert_job_role_strings_to_number_only()

    # TODO: 1794 - Placeholder only.
    # workplace_lf = apply_categorical_labels()

    utils.sink_to_parquet(
        lazy_df=workplace_lf,
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
