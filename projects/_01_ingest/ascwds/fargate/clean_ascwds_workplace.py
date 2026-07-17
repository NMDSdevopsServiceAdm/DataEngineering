import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import expressions as expr
from polars_utils import utils
from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC

INT_COLUMNS: list[str] = [AWPClean.total_staff, AWPClean.worker_records]
BOUNDED_STAFF_COLUMNS: list[str] = [AWPClean.total_staff, AWPClean.worker_records]
MIN_VALID_STAFF_COUNT: int = 1

WORKPLACE_SCHEMA = {
    AWPClean.organisation_id: pl.String,
    AWPClean.period: pl.String,
    AWPClean.establishment_id: pl.String,
    AWPClean.establishment_id_from_nmds: pl.String,
    AWPClean.parent_id: pl.String,
    AWPClean.nmds_id: pl.String,
    AWPClean.establishment_created_date: pl.String,
    AWPClean.establishment_updated_date: pl.String,
    AWPClean.master_update_date: pl.String,
    AWPClean.last_logged_in: pl.String,
    AWPClean.la_permission: pl.String,
    AWPClean.is_bulk_uploader: pl.String,
    AWPClean.is_parent: pl.String,
    AWPClean.parent_permission: pl.String,
    AWPClean.registration_type: pl.String,
    AWPClean.provider_id: pl.String,
    AWPClean.location_id: pl.String,
    AWPClean.establishment_type: pl.String,
    AWPClean.establishment_name: pl.String,
    AWPClean.address: pl.String,
    AWPClean.postcode: pl.String,
    AWPClean.region_id: pl.String,
    AWPClean.total_staff: pl.String,
    AWPClean.worker_records: pl.String,
    AWPClean.total_starters: pl.String,
    AWPClean.total_leavers: pl.String,
    AWPClean.total_vacancies: pl.String,
    AWPClean.main_service_id: pl.String,
    AWPClean.version: pl.String,
    AWPClean.import_date: pl.String,
}

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
    lf = utils.scan_parquet(workplace_source, schema=WORKPLACE_SCHEMA).select(
        WORKPLACE_SCHEMA.keys()
    )

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

    # polars_streaming: job role columns are added/dropped across partitions over
    # time, so the schema is built from every file's own columns rather than a
    # fixed list - this is a metadata-only pass over file footers, not the data.
    combined_schema = utils.discover_combined_schema(workplace_source)
    slv_lf = utils.scan_parquet(workplace_source, schema=combined_schema).select(
        *[AWPClean.establishment_id, AWPClean.import_date],
        expr.is_slv_job_role_column(),
    )

    slv_lf = slv_lf.with_columns(pl.col(AWPClean.import_date).cast(pl.String))

    workplace_lf = workplace_lf.join(
        slv_lf, on=[AWPClean.establishment_id, AWPClean.import_date], how="left"
    ).drop(AWPClean.import_date)

    workplace_lf = workplace_lf.with_columns(
        pl.col(INT_COLUMNS).cast(pl.Int32, strict=False),
        expr.is_slv_job_role_column().cast(pl.Int32, strict=False),
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
