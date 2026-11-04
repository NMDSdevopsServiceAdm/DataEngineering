import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


def main(
    cleaned_ascwds_worker_source: str,
    cleaned_ascwds_workplace_source: str,
    flagged_destination: str,
) -> None:
    """Filters the cleaned ASC-WDS worker data, adds main service ID and flags leavers.

    Main service ID comes from the workplace snapshot with the same establishment
    ID and import date; it is null where there is no match. Only the join keys and
    main service ID are read from the workplace data.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        cleaned_ascwds_workplace_source (str): path to the cleaned ASC-WDS workplace data
        flagged_destination (str): destination for the flagged worker data
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)
    workplace_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=[
            AWPClean.establishment_id,
            AWPClean.ascwds_workplace_import_date,
            AWPClean.main_service_id,
        ],
    )

    worker_lf = wUtils.filter_worker_turnover_data(worker_lf)
    worker_lf = worker_lf.join(
        workplace_lf,
        left_on=[AWKClean.establishment_id, AWKClean.ascwds_worker_import_date],
        right_on=[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )
    worker_lf = wUtils.add_is_leaver_flag(worker_lf)

    utils.sink_to_parquet(worker_lf, output_path=flagged_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--flagged_destination",
            "Destination s3 directory for the filtered worker turnover data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        flagged_destination=args.flagged_destination,
    )
