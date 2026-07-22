import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.cleaning_utils import apply_categorical_labels


def main(
    cleaned_ascwds_workplace_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS workplace dataset and save it unchanged.

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    workplace_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)

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
