import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def main(
    cleaned_ascwds_worker_source: str,
    raw_ascwds_worker_source: str,
    flagged_destination: str,
) -> None:
    """Filters the cleaned ASC-WDS worker data, adds main service, flags leavers
    and adds raw worker columns.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        raw_ascwds_worker_source (str): path to the raw ASC-WDS worker data
        flagged_destination (str): destination for the flagged worker data
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)

    worker_lf = wUtils.filter_worker_turnover_data(worker_lf).drop(AWKClean.location_id)
    worker_lf = wUtils.add_main_service_type(worker_lf)
    worker_lf = wUtils.add_is_leaver_flag(worker_lf)

    raw_worker_lf = utils.scan_parquet(
        raw_ascwds_worker_source, schema=wUtils.RAW_WORKER_SCHEMA
    )
    worker_lf = wUtils.add_raw_worker_columns(worker_lf, raw_worker_lf)

    utils.sink_to_parquet(worker_lf, output_path=flagged_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--raw_ascwds_worker_source",
            "Source s3 directory for raw ascwds worker data",
        ),
        (
            "--flagged_destination",
            "Destination s3 directory for the filtered worker turnover data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        raw_ascwds_worker_source=args.raw_ascwds_worker_source,
        flagged_destination=args.flagged_destination,
    )
