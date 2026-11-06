import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

DEFAULT_MAX_MONTHS_SINCE_SAVE = 6


def main(
    cleaned_ascwds_worker_source: str,
    cleaned_ascwds_workplace_source: str,
    flagged_destination: str,
    n_months: int = DEFAULT_MAX_MONTHS_SINCE_SAVE,
) -> None:
    """Filters worker data, adds main service type and flags leavers and active establishments.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        cleaned_ascwds_workplace_source (str): path to the cleaned ASC-WDS
            workplace data, used for main service
        flagged_destination (str): destination for the flagged worker data
        n_months (int): most months since an establishment's latest save date for
            it to count as active
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)
    workplace_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=[
            AWPClean.establishment_id,
            AWPClean.ascwds_workplace_import_date,
            AWPClean.main_service_id_clean,
        ],
    )

    filtered_lf = wUtils.filter_worker_turnover_data(worker_lf)
    save_dates_lf = wUtils.get_latest_save_date_per_establishment(
        worker_lf, filtered_lf
    )

    filtered_lf = filtered_lf.drop(AWKClean.location_id)
    worker_lf = worker_lf.join(
        workplace_lf,
        left_on=[AWKClean.establishment_id, AWKClean.ascwds_worker_import_date],
        right_on=[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )
    filtered_lf = wUtils.add_main_service_type(filtered_lf)
    filtered_lf = wUtils.add_is_leaver_flag(filtered_lf)
    filtered_lf = wUtils.add_establishment_active_flag(
        filtered_lf, save_dates_lf, n_months
    )

    utils.sink_to_parquet(filtered_lf, output_path=flagged_destination)


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
        (
            "--n_months",
            "Most months since an establishment's latest save date to count as active",
            False,
            DEFAULT_MAX_MONTHS_SINCE_SAVE,
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        flagged_destination=args.flagged_destination,
        n_months=int(args.n_months),
    )
