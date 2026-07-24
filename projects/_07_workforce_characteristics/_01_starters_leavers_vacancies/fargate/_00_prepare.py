import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.cleaning_utils import apply_categorical_labels
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

INDEX_COLUMNS = [AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date]


def main(
    cleaned_ascwds_workplace_source: str,
    prepared_data_destination: str,
) -> None:
    """Reshape the cleaned ASCWDS workplace dataset's job-role columns from wide to long.

    Discovers the ASC-WDS job-role codes present in the source schema at runtime,
    then converts the 4-metrics-per-code wide SLV block into one row per
    (establishment_id, ascwds_workplace_import_date, job_role_code).

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    raw_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)
    job_role_columns = pUtils.discover_job_role_codes(raw_lf.collect_schema())

    # TODO: 1796 - Placeholder only.
    # pUtils.reduce_to_published_roles()

    slv_source_columns = [
        column
        for cols in job_role_columns
        for column in (cols.employees, cols.starters, cols.leavers, cols.vacancies)
    ]
    workplace_lf = raw_lf.select(*INDEX_COLUMNS, *slv_source_columns)

    job_roles_lf = pUtils.convert_job_role_columns_to_rows(
        workplace_lf, INDEX_COLUMNS, job_role_columns
    )

    # TODO: 1794 - Placeholder only.
    # job_roles_lf = apply_categorical_labels()

    utils.sink_to_parquet(
        lazy_df=job_roles_lf,
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
