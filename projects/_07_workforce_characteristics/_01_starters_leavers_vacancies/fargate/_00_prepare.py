import polars_utils.cleaning_utils as cUtils
import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils

unpublished_roles_mapping = {
    "101": ["2", "3", "5", "24", "45", "47", "49", "50"], # other managers
    "102": ["35", "37"], # other regulated professions
    "103": ["10", "11", "22", "23", "38"], # other direct care
    "104": ["25", "26", "27", "34", "36", "39", "40", "41", "42", "44", "46", "48", "51"], # other
} # fmt: skip


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

    workplace_lf = cUtils.merge_job_role_columns(
        workplace_lf, unpublished_roles_mapping
    )

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
