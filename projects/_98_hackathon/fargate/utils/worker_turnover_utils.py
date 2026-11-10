from datetime import date

import polars as pl

from polars_utils.cleaning_utils import (
    apply_categorical_labels,
    build_labels_lf,
    column_to_date,
)
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import (
    APPRENTICE,
    CARE_CERTIFICATE_STATUS,
    DISABLED,
    ETHNICITY,
    GENDER,
    LEVEL_SOCIAL_CARE_QUALIFICATION_HELD,
    SOCIAL_CARE_QUAL_HELD,
    SOURCE_OF_RECRUITMENT,
    ZERO_HOURS,
)
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.ascwds_labelled_vocab import (
    CountryOfBirthGroupLabels,
    MainServiceID,
    NationalityGroupLabels,
)
from utils.column_values.categorical_column_values import PrimaryServiceType

WORKER_CATEGORICAL_COLUMNS = [
    AWKClean.gender,
    AWKClean.ethnicity,
    AWKClean.nationality,
    AWKClean.country_of_birth,
    AWKClean.disabled,
    AWKClean.social_care_qual_held,
    AWKClean.level_social_care_qualification_held,
    AWKClean.care_certificate_status,
    AWKClean.apprentice,
    AWKClean.zero_hours,
    AWKClean.source_of_recruitment,
]

WORKER_NUMERIC_COLUMNS = [
    AWKClean.age,
    AWKClean.hourly_rate,
    AWKClean.salary,
    AWKClean.contracted_hours,
    AWKClean.average_hours,
    AWKClean.distance_to_work_in_miles,
    AWKClean.start_age,
    AWKClean.days_sick,
    AWKClean.start_in_sector,
    AWKClean.year_of_entry,
]

WORKER_DATE_COLUMNS = [
    AWKClean.created_date,
    AWKClean.start_date,
    AWKClean.pay_change_date,
    AWKClean.pay_save_date,
]

# Explicit, because a scan infers columns from the oldest file, which lacks newer ones.
RAW_WORKER_SCHEMA = {
    column: pl.String
    for column in [
        AWKClean.establishment_id,
        AWKClean.worker_id,
        AWKClean.import_date,
        *WORKER_CATEGORICAL_COLUMNS,
        *WORKER_NUMERIC_COLUMNS,
        *WORKER_DATE_COLUMNS,
    ]
}


def add_raw_worker_columns(
    worker_lf: pl.LazyFrame, raw_worker_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Adds raw worker columns from the worker's own snapshot.

    Left joins on establishment ID, worker ID and import date. Raw rows are cut
    to the keys in `worker_lf` by a semi join before casting, so the full raw
    history is never cast or joined. Negative values (not known/recorded) in code
    and numeric columns become null. Code columns become categorical and numeric
    columns `Float32` (about 7 significant digits; unparseable becomes null).
    Date columns are parsed from `dd/mm/yyyy` (blank becomes null; another shape
    raises; a two-digit year parses literally, so `01/03/15` is year 15). Relies
    on unique keys, which the cleaned data's validation checks.

    Args:
        worker_lf (pl.LazyFrame): Flagged worker data with establishment ID,
            worker ID and `ascwds_worker_import_date`.
        raw_worker_lf (pl.LazyFrame): Raw ASC-WDS worker data, all strings.

    Returns:
        pl.LazyFrame: The input rows with all columns plus the raw worker columns.
    """
    keys = [
        AWKClean.establishment_id,
        AWKClean.worker_id,
        AWKClean.ascwds_worker_import_date,
    ]

    raw_worker_lf = column_to_date(
        raw_worker_lf.select(RAW_WORKER_SCHEMA.keys()),
        AWKClean.import_date,
        AWKClean.ascwds_worker_import_date,
    ).drop(AWKClean.import_date)

    raw_worker_lf = raw_worker_lf.with_columns(
        pl.col(AWKClean.establishment_id).cast(
            CategoricalColumnTypes.EstablishmentCatType
        )
    ).join(worker_lf.select(keys), on=keys, how="semi")

    day_first_format = "%d/%m/%Y"

    code_columns = pl.col(WORKER_CATEGORICAL_COLUMNS)
    numeric_columns = pl.col(WORKER_NUMERIC_COLUMNS).cast(pl.Float32, strict=False)

    # Codes stay text, so match negatives on the string.
    raw_worker_lf = raw_worker_lf.with_columns(
        pl.when(~code_columns.str.starts_with("-"))
        .then(code_columns)
        .cast(pl.Categorical),
        pl.when(numeric_columns >= 0).then(numeric_columns),
        pl.col(WORKER_DATE_COLUMNS).replace("", None).str.to_date(day_first_format),
    )

    return worker_lf.join(raw_worker_lf, on=keys, how="left")


def add_worker_labels(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds a categorical `<column>_labels` column beside each coded worker column.

    Codes with no label keep their raw code in the label column. Null codes stay null.

    Args:
        worker_lf (pl.LazyFrame): Worker data with the raw coded worker columns.

    Returns:
        pl.LazyFrame: The input with a `_labels` column for each coded column.
    """
    code_to_label = {
        AWKClean.gender: GENDER.code_to_label(),
        AWKClean.ethnicity: ETHNICITY.code_to_label(),
        AWKClean.disabled: DISABLED.code_to_label(),
        AWKClean.social_care_qual_held: SOCIAL_CARE_QUAL_HELD.code_to_label(),
        AWKClean.level_social_care_qualification_held: (
            LEVEL_SOCIAL_CARE_QUALIFICATION_HELD.code_to_label()
        ),
        AWKClean.care_certificate_status: CARE_CERTIFICATE_STATUS.code_to_label(),
        AWKClean.apprentice: APPRENTICE.code_to_label(),
        AWKClean.zero_hours: ZERO_HOURS.code_to_label(),
        AWKClean.source_of_recruitment: SOURCE_OF_RECRUITMENT.code_to_label(),
    }

    worker_lf = apply_categorical_labels(
        worker_lf,
        build_labels_lf(code_to_label),
        list(code_to_label),
        add_as_new_column=True,
    )

    label_columns = [f"{column}_{DLC.label}s" for column in code_to_label]
    return worker_lf.with_columns(pl.col(label_columns).cast(pl.Categorical))


def combine_pay_and_hours(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Combines pay and hours into `hours` and `hourly_pay`, dropping the raw columns.

    - `hours` is the first of contracted hours then average hours that is above
      0 (raw hours can be 0), else null.
    - `hourly_pay` is the hourly rate where given, else the salary converted to
      hourly pay, null when hours is null.

    A worker has an hourly rate or a salary, never both, so null checks choose
    between them; if both were given, the rate is used. Rates of 0 are kept.

    Args:
        worker_lf (pl.LazyFrame): Worker data with the raw hourly rate, salary,
            contracted hours and average hours.

    Returns:
        pl.LazyFrame: The input rows with `hours` and `hourly_pay` in place of
            the raw hourly rate, salary, contracted hours and average hours.
    """
    weeks_per_year = 52

    contracted_hours = pl.col(AWKClean.contracted_hours)
    average_hours = pl.col(AWKClean.average_hours)

    worker_lf = worker_lf.with_columns(
        pl.coalesce(
            pl.when(contracted_hours > 0).then(contracted_hours),
            pl.when(average_hours > 0).then(average_hours),
        ).alias(HackCols.hours)
    )

    annual_salary_to_hourly_pay = pl.col(AWKClean.salary) / (
        pl.col(HackCols.hours) * weeks_per_year
    )

    return worker_lf.with_columns(
        pl.coalesce(AWKClean.hourly_rate, annual_salary_to_hourly_pay).alias(
            HackCols.hourly_pay
        )
    ).drop(
        AWKClean.hourly_rate,
        AWKClean.salary,
        AWKClean.contracted_hours,
        AWKClean.average_hours,
    )


def add_country_code_group(worker_lf: pl.LazyFrame, code_column: str) -> pl.LazyFrame:
    """Adds a categorical group column from an ISO 3166-1 numeric country code.

    The UK code gives the UK label, an EEA code (EU, Iceland, Liechtenstein or
    Norway) gives the EEA label and any other code gives "Non-EEA". A null or
    blank code gives null.

    Args:
        worker_lf (pl.LazyFrame): Worker data with the code column.
        code_column (str): `AWKClean.nationality` (adds `nationality_group`) or
            `AWKClean.country_of_birth` (adds `country_of_birth_group`).

    Returns:
        pl.LazyFrame: The input rows with all columns plus the group column.

    Raises:
        ValueError: If `code_column` is not a supported column.
    """
    group_columns_and_labels = {
        AWKClean.nationality: (
            HackCols.nationality_group,
            NationalityGroupLabels.british,
            NationalityGroupLabels.eea_non_british,
            NationalityGroupLabels.non_eea,
        ),
        AWKClean.country_of_birth: (
            HackCols.country_of_birth_group,
            CountryOfBirthGroupLabels.uk,
            CountryOfBirthGroupLabels.eea_non_uk,
            CountryOfBirthGroupLabels.non_eea,
        ),
    }
    if code_column not in group_columns_and_labels:
        raise ValueError(f"Unsupported country code column: {code_column}")
    group_column, uk_label, eea_label, non_eea_label = group_columns_and_labels[
        code_column
    ]

    # ISO 3166-1 numeric codes, unpadded as held in the raw data.
    uk_code = "826"
    eea_codes = [
        "40",  # Austria
        "56",  # Belgium
        "100",  # Bulgaria
        "191",  # Croatia
        "196",  # Cyprus
        "203",  # Czechia
        "208",  # Denmark
        "233",  # Estonia
        "246",  # Finland
        "250",  # France
        "276",  # Germany
        "300",  # Greece
        "348",  # Hungary
        "352",  # Iceland
        "372",  # Ireland
        "380",  # Italy
        "428",  # Latvia
        "438",  # Liechtenstein
        "440",  # Lithuania
        "442",  # Luxembourg
        "470",  # Malta
        "528",  # Netherlands
        "578",  # Norway
        "616",  # Poland
        "620",  # Portugal
        "642",  # Romania
        "703",  # Slovakia
        "705",  # Slovenia
        "724",  # Spain
        "752",  # Sweden
    ]
    code = pl.col(code_column).cast(pl.String).replace("", None)

    return worker_lf.with_columns(
        pl.when(code.is_null())
        .then(None)
        .when(code == uk_code)
        .then(pl.lit(uk_label))
        .when(code.is_in(eea_codes))
        .then(pl.lit(eea_label))
        .otherwise(pl.lit(non_eea_label))
        .cast(pl.Categorical)
        .alias(group_column)
    )


def filter_worker_turnover_data(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filters worker data to the rows kept for turnover analysis.

    Kept rows:
    - Import date on or after 1 January 2020.
    - Location ID is not null.
    - Employment status is permanent or temporary.
    - Import date is the earliest in its month, after the other filters. This
      uses `.over()`, which the streaming engine does not cover, so the sink may
      fall back to the in-memory engine.

    Args:
        worker_lf (pl.LazyFrame): Cleaned ASC-WDS worker data.

    Returns:
        pl.LazyFrame: The filtered worker data with all input columns.
    """
    start_date = date(2020, 1, 1)

    return worker_lf.filter(
        pl.col(AWKClean.ascwds_worker_import_date) >= start_date,
        pl.col(AWKClean.location_id).is_not_null(),
        pl.col(AWKClean.employment_status_clean_labelled).is_in(
            [EmpStat.permanent, EmpStat.temporary]
        ),
    ).filter(earliest_file_per_month_filter_expr(AWKClean.ascwds_worker_import_date))


def get_latest_save_date_per_establishment(
    worker_lf: pl.LazyFrame, snapshot_worker_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Finds the latest worker save date per establishment and snapshot.

    Uses workers of every employment status. Snapshots are the import dates
    in `snapshot_worker_lf`, semi-joined before aggregating to keep the
    aggregate small.

    Args:
        worker_lf (pl.LazyFrame): Unfiltered cleaned ASC-WDS worker data.
        snapshot_worker_lf (pl.LazyFrame): Output of
            `filter_worker_turnover_data`.

    Returns:
        pl.LazyFrame: Establishment ID, import date and `latest_save_date`
            (null if every save date is null), one row per establishment and
            snapshot.
    """
    snapshot_dates_lf = snapshot_worker_lf.select(
        AWKClean.ascwds_worker_import_date
    ).unique()

    return (
        worker_lf.join(
            snapshot_dates_lf, on=AWKClean.ascwds_worker_import_date, how="semi"
        )
        .group_by(AWKClean.establishment_id, AWKClean.ascwds_worker_import_date)
        .agg(
            pl.col(AWKClean.ascwds_worker_save_date)
            .max()
            .alias(HackCols.latest_save_date)
        )
    )


def add_establishment_active_flag(
    worker_lf: pl.LazyFrame, save_dates_lf: pl.LazyFrame, n_months: int
) -> pl.LazyFrame:
    """Flags workers whose establishment saved data recently at both snapshots.

    A gap is the difference in calendar months between the establishment's
    latest save date and the import date (31 Dec to 1 Jan is 1), at the start
    snapshot and the snapshot 12 months later. It is negative if the save date
    is after the import date, and null with no save date. `establishment_active` is true when both gaps are at
    most `n_months`, false when either gap is null, and null where `is_leaver`
    is null. Expects one snapshot per month. Gaps are built on the small save
    date frame so only one join reaches the worker rows.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `is_leaver`, one snapshot
            per month.
        save_dates_lf (pl.LazyFrame): Latest save date per establishment and
            snapshot import date, from `get_latest_save_date_per_establishment`.
        n_months (int): Most months since the latest save date for an
            establishment to count as active.

    Returns:
        pl.LazyFrame: The input rows with `months_since_save_start`,
            `months_since_save_end` and `establishment_active` added.
    """
    keys = [AWKClean.establishment_id, AWKClean.ascwds_worker_import_date]
    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    save_date = pl.col(HackCols.latest_save_date)

    save_gap_lf = save_dates_lf.with_columns(
        import_date.dt.truncate("1mo").alias(HackCols.import_month),
        (
            (import_date.dt.year() - save_date.dt.year()) * 12
            + (import_date.dt.month() - save_date.dt.month())
        ).alias(HackCols.months_since_save_start),
    )

    start_gap_lf = save_gap_lf.select(
        *keys,
        pl.col(HackCols.import_month).dt.offset_by("12mo").alias(HackCols.end_month),
        HackCols.months_since_save_start,
    )
    end_gap_lf = save_gap_lf.select(
        AWKClean.establishment_id,
        pl.col(HackCols.import_month).alias(HackCols.end_month),
        pl.col(HackCols.months_since_save_start).alias(HackCols.months_since_save_end),
    )
    gap_lf = start_gap_lf.join(
        end_gap_lf, on=[AWKClean.establishment_id, HackCols.end_month], how="left"
    ).drop(HackCols.end_month)

    worker_lf = worker_lf.join(gap_lf, on=keys, how="left")

    both_gaps_within_limit = (
        (pl.col(HackCols.months_since_save_start) <= n_months)
        & (pl.col(HackCols.months_since_save_end) <= n_months)
    ).fill_null(False)

    return worker_lf.with_columns(
        pl.when(pl.col(HackCols.is_leaver).is_null())
        .then(None)
        .otherwise(both_gaps_within_limit)
        .alias(HackCols.establishment_active)
    )


def add_turnover_rate(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds `turnover_rate`: leavers / start employees per establishment-month.

    Repeated on every worker row of the establishment-month. Null where the
    establishment is not in both snapshots. Uses `.over()`, which the streaming
    engine does not cover, so the sink may fall back to the in-memory engine.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `is_leaver`, one snapshot per
            month.

    Returns:
        pl.LazyFrame: The input rows with `turnover_rate` (`Float32`) added.
    """
    establishment_month = [
        AWKClean.establishment_id,
        AWKClean.ascwds_worker_import_date,
    ]

    return worker_lf.with_columns(
        pl.when(pl.col(HackCols.establishment_in_both_snapshots))
        .then(
            pl.col(HackCols.is_leaver).mean().over(establishment_month).cast(pl.Float32)
        )
        .alias(HackCols.turnover_rate)
    )


def add_headcount_reconciliation_gap(
    worker_lf: pl.LazyFrame, max_gap: int
) -> pl.LazyFrame:
    """Adds the headcount reconciliation gap and a within-tolerance flag.

    `headcount_reconciliation_gap` per establishment and start month is start
    headcount + workers created after the start import date - leavers - end
    headcount. The end snapshot is 12 months after the start. Created workers
    are counted on end snapshot rows by `created_date`, which is never after the
    snapshot's import date; a null `created_date` is not counted. Zero means the
    stayer and leaver labels reconcile. The gap is null where the establishment
    has no end snapshot. `headcount_gap_within_tolerance` is true when the gap is
    at most `max_gap` in either direction, and null when the gap is null.

    Counts are built on a narrow frame and aggregated to establishment-month, so
    one join reaches the worker rows. Expects one import date per month.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `is_leaver` and `created_date`.
        max_gap (int): Largest absolute gap, in workers, that counts as within
            tolerance.

    Returns:
        pl.LazyFrame: The input rows with `headcount_reconciliation_gap` (`Int32`)
            and `headcount_gap_within_tolerance` added.
    """
    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    import_month = pl.col(HackCols.import_month)
    keys = [AWKClean.establishment_id, HackCols.import_month]

    snapshot_lf = worker_lf.select(
        AWKClean.establishment_id,
        import_date,
        AWKClean.created_date,
        HackCols.is_leaver,
    ).with_columns(import_date.dt.truncate("1mo").alias(HackCols.import_month))

    start_lf = snapshot_lf.group_by(keys).agg(
        pl.len().cast(pl.Int32).alias(HackCols.start_headcount),
        pl.col(HackCols.is_leaver).sum().cast(pl.Int32).alias(HackCols.leaver_count),
    )

    start_dates_lf = (
        snapshot_lf.select(
            import_month.dt.offset_by("12mo"),
            import_date.alias(HackCols.start_import_date),
        )
        .unique()
        .rename({HackCols.import_month: HackCols.end_month})
    )

    end_lf = (
        snapshot_lf.join(
            start_dates_lf, left_on=HackCols.import_month, right_on=HackCols.end_month
        )
        .group_by(keys)
        .agg(
            pl.len().cast(pl.Int32).alias(HackCols.end_headcount),
            (pl.col(AWKClean.created_date) > pl.col(HackCols.start_import_date))
            .sum()
            .cast(pl.Int32)
            .alias(HackCols.created_count),
        )
        .with_columns(import_month.dt.offset_by("-12mo"))
    )

    gap_lf = (
        start_lf.join(end_lf, on=keys, how="left")
        .with_columns(
            (
                pl.col(HackCols.start_headcount)
                + pl.col(HackCols.created_count)
                - pl.col(HackCols.leaver_count)
                - pl.col(HackCols.end_headcount)
            ).alias(HackCols.headcount_reconciliation_gap)
        )
        .select(
            *keys,
            HackCols.headcount_reconciliation_gap,
            (pl.col(HackCols.headcount_reconciliation_gap).abs() <= max_gap).alias(
                HackCols.headcount_gap_within_tolerance
            ),
        )
    )

    return (
        worker_lf.with_columns(
            import_date.dt.truncate("1mo").alias(HackCols.import_month)
        )
        .join(gap_lf, on=keys, how="left")
        .drop(HackCols.import_month)
    )


def add_valid_for_analysis_flag(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds `plausible_turnover` and `valid_for_analysis`.

    `plausible_turnover` is false for a `turnover_rate` of:
    - 0: no leavers over the whole year.
    - 1: everyone left within the year.

    It is true for any rate between them and null when the rate is null.
    `valid_for_analysis` is true only when `establishment_in_both_snapshots`,
    `establishment_active`, `plausible_turnover` and
    `headcount_gap_within_tolerance` are all true, otherwise false. Keeps every
    row.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `turnover_rate`,
            `establishment_in_both_snapshots`, `establishment_active` and
            `headcount_gap_within_tolerance`.

    Returns:
        pl.LazyFrame: The input rows with `plausible_turnover` and
            `valid_for_analysis` added.
    """
    return worker_lf.with_columns(
        # Endpoints excluded: 0 and 1 are the implausible rates.
        pl.col(HackCols.turnover_rate)
        .is_between(0, 1, closed="none")
        .alias(HackCols.plausible_turnover)
    ).with_columns(
        (
            pl.col(HackCols.establishment_in_both_snapshots)
            & pl.col(HackCols.establishment_active)
            & pl.col(HackCols.plausible_turnover)
            & pl.col(HackCols.headcount_gap_within_tolerance)
        )
        .fill_null(False)
        .alias(HackCols.valid_for_analysis)
    )


def add_main_service_type(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds `main_service_type`, a three-way recode of the main service label.

    Values:
    - "Care home with nursing" for the CHN main service.
    - "Care home without nursing" for the CHS main service.
    - "non-residential" for any other main service.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `main_service_id_clean`.

    Returns:
        pl.LazyFrame: The input rows with all columns plus `main_service_type`.
    """
    main_service = pl.col(AWPClean.main_service_id_clean)

    return worker_lf.with_columns(
        pl.when(main_service == MainServiceID.care_home_services_with_nursing_chn)
        .then(pl.lit(PrimaryServiceType.care_home_with_nursing))
        .when(main_service == MainServiceID.care_home_services_without_nursing_chs)
        .then(pl.lit(PrimaryServiceType.care_home_only))
        .otherwise(pl.lit(PrimaryServiceType.non_residential))
        .cast(CategoricalColumnTypes.PrimaryServiceEnumType)
        .alias(HackCols.main_service_type)
    )


def add_is_leaver_flag(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Flags leavers and establishments present in the snapshot 12 months later.

    Months are matched on month and year. A worker is an establishment ID and
    worker ID pair. `establishment_in_both_snapshots` is true when the
    establishment has a worker in the later snapshot; where false, `is_leaver`
    is null as absence says nothing about leaving. Both are null where no
    snapshot exists 12 months later. Semi and anti joins keep each input row
    once, including duplicates; `pl.concat` does not preserve row order.

    Args:
        worker_lf (pl.LazyFrame): Filtered worker data, one snapshot per month.

    Returns:
        pl.LazyFrame: The input rows with all columns plus
            `establishment_in_both_snapshots` and `is_leaver`.
    """
    worker_keys = [AWKClean.establishment_id, AWKClean.worker_id]

    worker_lf = worker_lf.with_columns(
        pl.col(AWKClean.ascwds_worker_import_date)
        .dt.truncate("1mo")
        .alias(HackCols.import_month)
    )
    end_lf = worker_lf.select(worker_keys + [HackCols.import_month])
    end_months_lf = end_lf.select(HackCols.import_month).unique()

    start_lf = worker_lf.with_columns(
        pl.col(HackCols.import_month).dt.offset_by("12mo").alias(HackCols.end_month)
    )

    no_end_snapshot_lf = start_lf.join(
        end_months_lf,
        left_on=HackCols.end_month,
        right_on=HackCols.import_month,
        how="anti",
    ).with_columns(
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.is_leaver),
    )
    start_lf = start_lf.join(
        end_months_lf,
        left_on=HackCols.end_month,
        right_on=HackCols.import_month,
        how="semi",
    )

    end_establishments_lf = end_lf.select(
        AWKClean.establishment_id, HackCols.import_month
    ).unique()
    establishment_start_keys = [AWKClean.establishment_id, HackCols.end_month]
    establishment_end_keys = [AWKClean.establishment_id, HackCols.import_month]

    establishment_missing_lf = start_lf.join(
        end_establishments_lf,
        left_on=establishment_start_keys,
        right_on=establishment_end_keys,
        how="anti",
    ).with_columns(
        pl.lit(False).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.is_leaver),
    )
    start_lf = start_lf.join(
        end_establishments_lf,
        left_on=establishment_start_keys,
        right_on=establishment_end_keys,
        how="semi",
    )

    start_keys = worker_keys + [HackCols.end_month]
    end_keys = worker_keys + [HackCols.import_month]

    stayers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="semi"
    ).with_columns(
        pl.lit(True).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(False).alias(HackCols.is_leaver),
    )
    leavers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="anti"
    ).with_columns(
        pl.lit(True).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(True).alias(HackCols.is_leaver),
    )

    return pl.concat(
        [stayers_lf, leavers_lf, establishment_missing_lf, no_end_snapshot_lf]
    ).drop(HackCols.import_month, HackCols.end_month)
