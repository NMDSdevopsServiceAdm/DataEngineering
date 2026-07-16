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

RECONCILIATION_COLUMNS = [
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

jr_nums_list = [i for i in range(1, 53)]


def main(
    workplace_source: str,
    data_labels_source: str,
    worker_source: str,
    cleaned_workplace_destination: str,
    workplace_for_reconciliation_destination: str,
) -> None:
    """
    Clean raw AWS-WDS data.

    Args:
        workplace_source (str): path to the raw ascwds workplace data
        data_labels_source (str): path to the ascwdsdata labels source
        worker_source (str): path to the raw ascwds worker data
        cleaned_workplace_destination (str): destination for cleaned ascwds workplace output
        workplace_for_reconciliation_destination (str): destination for reconciliation workplace output
    """
    lf = utils.scan_parquet(workplace_source, schema=WORKPLACE_SCHEMA)

    lf = lf.select(WORKPLACE_SCHEMA.keys())  # Removes partition columns from output.

    lf = wUtils.apply_data_corrections(lf)

    lf = lf.filter(wUtils.valid_workplace_filter())

    lf = lf.rename({AWPClean.last_logged_in: AWPClean.last_logged_in_date})

    lf = cUtils.cast_date_strings_to_dates(lf)

    lf = cUtils.column_to_date(
        lf, AWPClean.import_date, AWPClean.ascwds_workplace_import_date
    )

    # trello 1705
    data_labels_lf = pl.scan_csv(data_labels_source, schema=data_labels_schema)
    lf = cUtils.apply_categorical_labels(
        lf,
        data_labels_lf,
        columns_to_apply_labels,
        add_as_new_column=False,
    )

    (
        lf,
        reconciliation_lf,
    ) = wUtils.create_purged_lfs_for_reconciliation_and_data(lf)

    lf = wUtils.remove_rows_with_duplicate_location_ids(lf)

    worker_lf = utils.scan_parquet(worker_source)
    wUtils.check_job_roles_list(worker_lf, jr_nums_list)
    lf_slv = utils.scan_parquet(
        workplace_source, schema=wUtils.create_slv_schema(jr_nums_list, incl_index=True)
    ).drop([AWPClean.version, AWPClean.year, AWPClean.month, AWPClean.day])

    lf = lf.join(
        lf_slv, on=[AWPClean.establishment_id, AWPClean.import_date], how="left"
    ).drop(AWPClean.import_date)

    lf = lf.with_columns(
        pl.col(INT_COLUMNS).cast(pl.Int32, strict=False),
        expr.is_slv_job_role_column().cast(pl.Int32, strict=False),
    )

    lf = lf.with_columns(
        pl.when(pl.col(BOUNDED_STAFF_COLUMNS) >= MIN_VALID_STAFF_COUNT)
        .then(pl.col(BOUNDED_STAFF_COLUMNS))
        .otherwise(None)
        .name.suffix("_bounded")
    )

    reconciliation_lf = reconciliation_lf.select(RECONCILIATION_COLUMNS)

    print(
        f"Exporting clean ascwds workplace data as parquet to {cleaned_workplace_destination}"
    )
    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=cleaned_workplace_destination,
    )

    print(
        f"Exporting ascwds workplace reconciliation data as parquet to {workplace_for_reconciliation_destination}"
    )
    utils.sink_to_parquet(
        lazy_df=reconciliation_lf,
        output_path=workplace_for_reconciliation_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--workplace_source",
            "Source s3 directory for raw ascwds workplace data",
        ),
        (
            "--data_labels_source",
            "Source s3 directory for ascwds data labels",
        ),
        (
            "--worker_source",
            "Source s3 directory for raw ascwds worker data",
        ),
        (
            "--cleaned_workplace_destination",
            "Destination s3 directory for cleaned ascwds workplace output",
        ),
        (
            "--workplace_for_reconciliation_destination",
            "Destination s3 directory for reconciliation workplace output",
        ),
    )
    main(
        workplace_source=args.workplace_source,
        data_labels_source=args.data_labels_source,
        worker_source=args.worker_source,
        cleaned_workplace_destination=args.cleaned_workplace_destination,
        workplace_for_reconciliation_destination=args.workplace_for_reconciliation_destination,
    )
