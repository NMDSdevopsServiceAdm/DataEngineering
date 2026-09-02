import polars as pl

from polars_utils import cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

NOT_KNOWN_JOB_ROLE = "-1"

LEGACY_JOB_ROLE_CODES = {"41": "40", "22": "27"}


def remove_workers_without_workplaces(
    worker_lf: pl.LazyFrame, workplace_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Removes worker records that do not have a corresponding workplace record.

    Workplaces are cleaned during the workplace cleaning process, so if a
    workplace has been removed then the worker records for that workplace
    should also be removed.

    Args:
        worker_lf (pl.LazyFrame): worker records.
        workplace_lf (pl.LazyFrame): cleaned workplace records.

    Returns:
        pl.LazyFrame: worker records that have a corresponding workplace record.
    """
    workplace_lf = workplace_lf.select(AWPClean.import_date, AWPClean.establishment_id)

    return worker_lf.join(
        workplace_lf,
        left_on=[AWKClean.import_date, AWKClean.establishment_id],
        right_on=[AWPClean.import_date, AWPClean.establishment_id],
        how="semi",
    )


def remap_mainjrid_codes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Recodes job roles by replacing raw mainjrid codes.

    Over time, some job roles have stopped being collected by the ASC-WDS.
    Worker records with these roles were recoded into different roles. For
    consistency across all periods, this function applies that mapping across
    all periods. The notes below show the date at which roles were removed
    from the ASC-WDS:
        - May 2024: 'Care navigator': 'Care co-ordinator'
        - May 2024: 'Technician': 'Other non-care related staff'

    Args:
        lf (pl.LazyFrame): LazyFrame containing the main job role column.

    Returns:
        pl.LazyFrame: LazyFrame with the replaced value.
    """
    return lf.with_columns(
        pl.col(AWKClean.main_job_role_clean).replace(LEGACY_JOB_ROLE_CODES)
    )


def impute_not_known_job_roles(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Imputes not known job roles by filling with known values from other import dates.

    Replaces 'not known' (`"-1"`) job roles with the most recent past known
    value for that worker, or, where none exists (the role was never known
    before that point), the nearest future known value instead. Rows where a
    worker's job role is never known on any import date keep the 'not known'
    value.

    Performance note: the two `.over()` calls below are not yet covered by
    Polars' streaming engine and fall back to the in-memory engine (confirmed
    via a `POLARS_VERBOSE=1` run against production-scale data, not just
    inferred from the `polars-streaming-check` skill's tracking-issue list).
    This hasn't caused an OOM on this clean step.

    Args:
        lf (pl.LazyFrame): LazyFrame containing `worker_id`,
            `ascwds_worker_import_date` and `main_job_role_clean`.

    Returns:
        pl.LazyFrame: LazyFrame with the `main_job_role_clean` column with
            imputed values.
    """
    lf = lf.with_columns(
        pl.when(pl.col(AWKClean.main_job_role_clean) == NOT_KNOWN_JOB_ROLE)
        .then(None)
        .otherwise(pl.col(AWKClean.main_job_role_clean))
        .alias(AWKClean.main_job_role_clean)
    )
    lf = lf.with_columns(
        pl.col(AWKClean.main_job_role_clean)
        .forward_fill()
        .over(AWKClean.worker_id, order_by=AWKClean.ascwds_worker_import_date)
    )
    lf = lf.with_columns(
        pl.col(AWKClean.main_job_role_clean)
        .backward_fill()
        .over(AWKClean.worker_id, order_by=AWKClean.ascwds_worker_import_date)
    )
    return lf.with_columns(
        pl.col(AWKClean.main_job_role_clean).fill_null(NOT_KNOWN_JOB_ROLE)
    )


def create_clean_main_job_role_column(
    lf: pl.LazyFrame, data_labels_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Cleans the main job role column and adds its categorical labels as a new column.

    Args:
        lf (pl.LazyFrame): LazyFrame containing the original main job role column.
        data_labels_lf (pl.LazyFrame): LazyFrame mapping job role codes to labels.

    Returns:
        pl.LazyFrame: LazyFrame with the cleaned and labelled main job role columns.
    """
    lf = lf.with_columns(
        pl.col(AWKClean.main_job_role_id).alias(AWKClean.main_job_role_clean)
    )

    lf = remap_mainjrid_codes(lf)
    lf = impute_not_known_job_roles(lf)

    lf = lf.filter(pl.col(AWKClean.main_job_role_clean) != NOT_KNOWN_JOB_ROLE)

    return cUtils.apply_categorical_labels(
        lf,
        data_labels_lf,
        [AWKClean.main_job_role_clean],
        add_as_new_column=True,
    )
