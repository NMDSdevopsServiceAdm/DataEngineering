import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.clean_utils as cUtils
from polars_utils import utils


def main(
    merged_data_source: str,
    cleaned_data_destination: str,
) -> None:
    """
    Cleans the merged data.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): destination for cleaned output
    """
    lf = utils.scan_parquet(merged_data_source)

    # TODO: Placeholder only
    # cUtils.deduplicate_slv_over_time()

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--cleaned_data_destination",
            "Destination s3 directory for cleaned data",
        ),
    )
    main(
        merged_data_source=args.merged_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
