import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils


def main(
    cleaned_ascwds_worker_source: str, flagged_destination: str, n_months: int = 6
) -> None:
    """Filters the cleaned ASC-WDS worker data and flags leavers and active establishments.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        flagged_destination (str): destination for the flagged worker data
        n_months (int): most months since an establishment's latest save date for
            it to count as active
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)

    save_dates_lf = wUtils.get_latest_save_date_per_establishment(worker_lf)

    worker_lf = wUtils.filter_worker_turnover_data(worker_lf)
    worker_lf = wUtils.add_is_leaver_flag(worker_lf)
    worker_lf = wUtils.add_establishment_active_flag(worker_lf, save_dates_lf, n_months)

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
        (
            "--n_months",
            "Most months since an establishment's latest save date to count as active",
            False,
            6,
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        flagged_destination=args.flagged_destination,
        n_months=int(args.n_months),
    )
