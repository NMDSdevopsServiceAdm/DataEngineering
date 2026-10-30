import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils


def main(cleaned_ascwds_worker_source: str, flagged_destination: str) -> None:
    """Filters the cleaned ASC-WDS worker data to the monthly worker turnover input.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        flagged_destination (str): destination for the filtered worker data
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)

    worker_lf = wUtils.filter_worker_turnover_data(worker_lf)

    utils.sink_to_parquet(worker_lf, output_path=flagged_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--flagged_destination",
            "Destination s3 directory for the filtered worker turnover data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        flagged_destination=args.flagged_destination,
    )
