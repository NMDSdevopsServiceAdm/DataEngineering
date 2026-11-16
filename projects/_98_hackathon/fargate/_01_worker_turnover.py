import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols


def main(
    cleaned_ascwds_worker_source: str,
    cleaned_ascwds_workplace_source: str,
    raw_ascwds_worker_source: str,
    train_destination: str,
    holdout_destination: str,
    test_destination: str,
    n_months: int = wUtils.DEFAULT_MAX_MONTHS_SINCE_SAVE,
    max_gap: int = wUtils.DEFAULT_MAX_HEADCOUNT_GAP,
) -> None:
    """Creates the worker-level dataset for the turnover hackathon.

    Combines ASC-WDS worker data with workplace information, adds the leaver flags
    and workplace turnover measures needed to analyse and model worker turnover.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        cleaned_ascwds_workplace_source (str): path to the cleaned ASC-WDS
            workplace data, used for main service
        raw_ascwds_worker_source (str): path to the raw ASC-WDS worker data
        train_destination (str): destination for the train split
        holdout_destination (str): destination for the holdout split
        test_destination (str): destination for the test split
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
        AWKClean.location_id,
        AWKClean.main_job_role_id,
        AWKClean.main_job_role_clean,
        AWKClean.employment_status,
        AWKClean.employment_status_clean,
    )
    filtered_lf = filtered_lf.join(
        workplace_lf,
        left_on=[AWKClean.establishment_id, AWKClean.ascwds_worker_import_date],
        right_on=[AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )
    filtered_lf = wUtils.add_main_service_type(filtered_lf).drop(
        AWPClean.main_service_id_clean
    )

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
    filtered_lf = wUtils.add_valid_for_analysis_flag(filtered_lf).drop(
        HackCols.establishment_in_both_snapshots,
        HackCols.months_since_save_start,
        HackCols.months_since_save_end,
        HackCols.establishment_active,
        HackCols.headcount_reconciliation_gap,
        HackCols.headcount_gap_within_tolerance,
        HackCols.plausible_turnover,
    )
    filtered_lf = wUtils.null_not_known_codes(filtered_lf)
    filtered_lf = filtered_lf.with_columns(
        pl.col(wUtils.WORKER_CATEGORICAL_COLUMNS).cast(pl.Categorical)
    )
    filtered_lf = wUtils.combine_pay_and_hours(filtered_lf)
    filtered_lf = wUtils.add_worker_labels(filtered_lf)
    filtered_lf = wUtils.add_country_code_group(filtered_lf, AWKClean.nationality)
    filtered_lf = wUtils.add_country_code_group(
        filtered_lf, AWKClean.country_of_birth
    ).drop(wUtils.WORKER_CATEGORICAL_COLUMNS)
    filtered_lf = wUtils.impute_worker_columns(filtered_lf, wUtils.IMPUTE_COLUMNS)
    filtered_lf = wUtils.add_nlw_and_pay_difference(filtered_lf)

    filtered_lf = wUtils.add_split_column(filtered_lf)

    split_destinations = {
        "train": train_destination,
        "holdout": holdout_destination,
        "test": test_destination,
    }
    # Lazy sinks collected together share one run of the plan above, rather than
    # re-running it per split.
    split_sinks = [
        utils.sink_to_parquet(
            filtered_lf.filter(pl.col(HackCols.split) == split),
            output_path=destination,
            lazy=True,
        )
        for split, destination in split_destinations.items()
    ]
    pl.collect_all(split_sinks, engine="streaming")


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
            "--train_destination",
            "Destination s3 directory for the train split",
        ),
        (
            "--holdout_destination",
            "Destination s3 directory for the holdout split",
        ),
        (
            "--test_destination",
            "Destination s3 directory for the test split",
        ),
        (
            "--n_months",
            "Most months since an establishment's latest save date to count as active",
            False,
            wUtils.DEFAULT_MAX_MONTHS_SINCE_SAVE,
        ),
        (
            "--max_gap",
            "Largest absolute headcount reconciliation gap to count as within tolerance",
            False,
            wUtils.DEFAULT_MAX_HEADCOUNT_GAP,
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        raw_ascwds_worker_source=args.raw_ascwds_worker_source,
        train_destination=args.train_destination,
        holdout_destination=args.holdout_destination,
        test_destination=args.test_destination,
        n_months=int(args.n_months),
        max_gap=int(args.max_gap),
    )
