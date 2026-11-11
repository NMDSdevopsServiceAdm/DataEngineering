import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

DEFAULT_MAX_MONTHS_SINCE_SAVE = 6
DEFAULT_MAX_HEADCOUNT_GAP = 0


def main(
    cleaned_ascwds_worker_source: str,
    cleaned_ascwds_workplace_source: str,
    raw_ascwds_worker_source: str,
    flagged_destination: str,
    n_months: int = DEFAULT_MAX_MONTHS_SINCE_SAVE,
    max_gap: int = DEFAULT_MAX_HEADCOUNT_GAP,
) -> None:
    """Creates the worker-level dataset for the turnover hackathon.

    Combines ASC-WDS worker data with workplace information, adds the leaver flags
    and workplace turnover measures needed to analyse and model worker turnover.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        cleaned_ascwds_workplace_source (str): path to the cleaned ASC-WDS
            workplace data, used for main service
        raw_ascwds_worker_source (str): path to the raw ASC-WDS worker data
        flagged_destination (str): destination for the flagged worker data
        n_months (int): most months since an establishment's latest save date for
            it to count as active
        max_gap (int): largest absolute headcount reconciliation gap, in workers,
            for an establishment-month to count as within tolerance
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

    filtered_lf = wUtils.filter_worker_turnover_data(worker_lf).drop(
        AWKClean.location_id
    )
    filtered_lf = filtered_lf.join(
        workplace_lf,
        left_on=[AWKClean.establishment_id, AWKClean.ascwds_worker_import_date],
        right_on=[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )
    filtered_lf = wUtils.add_main_service_type(filtered_lf)

    # The later steps use this frame several times. Collect once, so each use does
    # not re-scan and re-filter the cleaned data.
    filtered_lf = filtered_lf.collect().lazy()

    save_dates_lf = wUtils.get_latest_save_date_per_establishment(
        worker_lf, filtered_lf
    )
    filtered_lf = wUtils.add_is_leaver_flag(filtered_lf)
    filtered_lf = wUtils.add_establishment_active_flag(
        filtered_lf, save_dates_lf, n_months
    )
    filtered_lf = wUtils.add_turnover_rate(filtered_lf)

    raw_worker_lf = utils.scan_parquet(
        raw_ascwds_worker_source, schema=wUtils.RAW_WORKER_SCHEMA
    )
    filtered_lf = wUtils.add_raw_worker_columns(filtered_lf, raw_worker_lf)
    filtered_lf = wUtils.add_headcount_reconciliation_gap(filtered_lf, max_gap)
    filtered_lf = wUtils.add_valid_for_analysis_flag(filtered_lf)
    filtered_lf = wUtils.combine_pay_and_hours(filtered_lf)
    filtered_lf = wUtils.add_worker_labels(filtered_lf)
    filtered_lf = wUtils.add_country_code_group(filtered_lf, AWKClean.nationality)
    filtered_lf = wUtils.add_country_code_group(filtered_lf, AWKClean.country_of_birth)

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
            "--raw_ascwds_worker_source",
            "Source s3 directory for raw ascwds worker data",
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
        (
            "--max_gap",
            "Largest absolute headcount reconciliation gap to count as within tolerance",
            False,
            DEFAULT_MAX_HEADCOUNT_GAP,
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        raw_ascwds_worker_source=args.raw_ascwds_worker_source,
        flagged_destination=args.flagged_destination,
        n_months=int(args.n_months),
        max_gap=int(args.max_gap),
    )
