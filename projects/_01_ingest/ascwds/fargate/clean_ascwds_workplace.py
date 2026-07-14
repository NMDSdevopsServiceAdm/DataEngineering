import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC

INT_COLUMNS: list[str] = [AWPClean.total_staff, AWPClean.worker_records]
BOUNDED_STAFF_COLUMNS: list[str] = [AWPClean.total_staff, AWPClean.worker_records]
MIN_VALID_STAFF_COUNT: int = 1

COLUMNS_TO_IMPORT = [
    AWPClean.organisation_id,
    AWPClean.period,
    AWPClean.establishment_id,
    AWPClean.establishment_id_from_nmds,
    AWPClean.parent_id,
    AWPClean.nmds_id,
    AWPClean.establishment_created_date,
    AWPClean.establishment_updated_date,
    AWPClean.master_update_date,
    AWPClean.last_logged_in,
    AWPClean.la_permission,
    AWPClean.is_bulk_uploader,
    AWPClean.is_parent,
    AWPClean.parent_permission,
    AWPClean.registration_type,
    AWPClean.provider_id,
    AWPClean.location_id,
    AWPClean.establishment_type,
    AWPClean.establishment_name,
    AWPClean.address,
    AWPClean.postcode,
    AWPClean.region_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.total_starters,
    AWPClean.total_leavers,
    AWPClean.total_vacancies,
    AWPClean.main_service_id,
    AWPClean.version,
    AWPClean.import_date,
]

SFC_INTERNAL_COLUMNS = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.master_update_date,
    AWPClean.master_update_date_org,
    AWPClean.establishment_created_date,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.organisation_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.region_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.last_logged_in_date,
    AWPClean.la_permission,
]

columns_to_apply_labels = [
    AWPClean.establishment_type,
    AWPClean.parent_permission,
    AWPClean.is_parent,
    AWPClean.main_service_id,
    AWPClean.registration_type,
]

data_labels_schema = pl.Schema(
    [(DLC.column_name, pl.String), (DLC.code, pl.String), (DLC.label, pl.String)]
)


def main(
    workplace_source: str,
    data_labels_source: str,
    cleaned_workplace_destination: str,
    ascwds_for_sfc_internal_destination: str,
) -> None:
    """
    Clean raw AWS-WDS data.

    Args:
        workplace_source (str): path to the raw ASC-WDS workplace data
        data_labels_source (str): path to the ASC-WDS data labels source
        cleaned_workplace_destination (str): destination for cleaned ASC-WDS
            workplace output
        ascwds_for_sfc_internal_destination (str): destination for ASC-WDS data
            for SFC internal pipeline use
    """
    lf = utils.scan_parquet(workplace_source, selected_columns=COLUMNS_TO_IMPORT)

    lf = wUtils.apply_data_corrections(lf)

    lf = lf.filter(wUtils.valid_workplace_filter())

    lf = lf.rename({AWPClean.last_logged_in: AWPClean.last_logged_in_date})

    lf = cUtils.cast_date_strings_to_dates(lf)

    lf = cUtils.column_to_date(
        lf, AWPClean.import_date, AWPClean.ascwds_workplace_import_date
    )

    data_labels_lf = pl.scan_csv(data_labels_source, schema=data_labels_schema)
    lf = cUtils.apply_categorical_labels(
        lf,
        data_labels_lf,
        columns_to_apply_labels,
        add_as_new_column=False,
    )

    lf = wUtils.create_purge_date_columns(lf)

    # The LazyFrame is split into two at this point:
    # - SfC internal pipeline (filtered to workplaces last *active* on or after the purge date)
    # - Cleaned ASC-WDS workplace data (filtered to workplaces last *amended* on or after the purge date)

    # SfC Internal pipeline
    sfc_internal_lf = lf.filter(
        pl.col(AWPClean.workplace_last_active_date) >= pl.col(AWPClean.purge_date)
    ).select(SFC_INTERNAL_COLUMNS)

    utils.sink_to_parquet(
        sfc_internal_lf, output_path=ascwds_for_sfc_internal_destination
    )

    # Cleaned ASC-WDS workplace data
    workplace_lf = lf.filter(
        pl.col(AWPClean.data_last_amended_date) >= pl.col(AWPClean.purge_date)
    )

    workplace_lf = wUtils.remove_rows_with_duplicate_location_ids(workplace_lf)

    slv_lf = utils.scan_parquet(workplace_source).select(
        *[AWPClean.establishment_id, AWPClean.import_date], wUtils.slv_cols_selector()
    )

    workplace_lf = workplace_lf.join(
        slv_lf, on=[AWPClean.establishment_id, AWPClean.import_date], how="left"
    ).drop(AWPClean.import_date)

    workplace_lf = workplace_lf.with_columns(
        pl.col(INT_COLUMNS).cast(pl.Int32, strict=False),
        wUtils.slv_cols_selector().cast(pl.Int32, strict=False),
    )

    workplace_lf = workplace_lf.with_columns(
        pl.when(pl.col(BOUNDED_STAFF_COLUMNS) >= MIN_VALID_STAFF_COUNT)
        .then(pl.col(BOUNDED_STAFF_COLUMNS))
        .otherwise(None)
        .name.suffix("_bounded")
    )

    utils.sink_to_parquet(workplace_lf, output_path=cleaned_workplace_destination)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--workplace_source",
            "Source s3 directory for raw ASC-WDS workplace data",
        ),
        (
            "--data_labels_source",
            "Source s3 directory for ASC-WDS data labels",
        ),
        (
            "--cleaned_workplace_destination",
            "Destination s3 directory for cleaned ASC-WDS workplace output",
        ),
        (
            "--ascwds_for_sfc_internal_destination",
            "Destination s3 directory for ASC-WDS data for SFC internal pipeline use",
        ),
    )
    main(
        workplace_source=args.workplace_source,
        data_labels_source=args.data_labels_source,
        cleaned_workplace_destination=args.cleaned_workplace_destination,
        ascwds_for_sfc_internal_destination=args.ascwds_for_sfc_internal_destination,
    )
