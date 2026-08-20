"""THROWAWAY diagnostics prototype for ticket 1906's OOM investigation.

Not part of the real pipeline - never wired into an auto-triggered Step
Function, never merged to main. Delete this file, its terraform/Step
Function wiring, and clean_ascwds_workplace_prototype.py once the
investigation concludes.

Control/baseline run: reproduces `main`'s current clean_ascwds_workplace.py
and the establishment_id-duplicate part of clean_workplace_utils.py
(unmodified, pre-ticket-1906 - no combined-schema duplicate detection at
all, just the old hardcoded DUPLICATE_ESTABLISHMENT_IDS filter), the same
RunDiagnostics instrumentation as clean_ascwds_workplace_prototype.py.
Confirms whether the OOM is caused by 1906's new code alone, or whether
`main` is already close to the memory boundary.
"""

import polars as pl
import polars.selectors as cs

from polars_utils import cleaning_utils as cUtils
from polars_utils import expressions as expr
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.run_diagnostics import RunDiagnostics
from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.file_utils import split_s3_uri

# `main`'s establishment_id-duplicate exclusion, as it exists before ticket
# 1906 - not the branch's own wUtils.valid_workplace_filter(), which no
# longer does this. Kept inline here rather than reintroducing it into the
# real (already-changed) clean_workplace_utils.py.
DUPLICATE_ESTABLISHMENT_IDS: set[str] = {
    "48904",
    "49966",
    "49967",
    "49968",
    "50538",
    "50561",
    "50590",
    "50596",
    "50598",
    "50621",
    "50623",
    "50624",
    "50627",
    "50629",
    "50639",
    "50640",
    "50767",
    "50769",
    "50770",
    "50771",
    "50869",
    "50870",
}


def baseline_valid_workplace_filter() -> pl.Expr:
    """`main`'s pre-1906 valid_workplace_filter() - test accounts + hardcoded duplicate IDs."""
    return wUtils.valid_workplace_filter() & ~pl.col(AWPClean.establishment_id).is_in(
        DUPLICATE_ESTABLISHMENT_IDS
    )


bounds = wUtils.BoundingExpressions()

INT_COLUMNS: list[str] = [AWPClean.total_staff, AWPClean.worker_records]

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

legacy_job_roles_dict = {
    "27": ["22"],
    "40": ["41"],
    "42": ["12", "13", "14", "18", "19", "20", "21"],
}


def main(
    workplace_source: str,
    data_labels_source: str,
    cleaned_workplace_destination: str,
    ascwds_for_sfc_internal_destination: str,
) -> None:
    """Clean raw ASC-WDS data - baseline/control copy, see module docstring."""
    data_bucket, _ = split_s3_uri(cleaned_workplace_destination)
    diagnostics = RunDiagnostics(
        "ascwds_workplace_prototype_baseline", data_bucket
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        lf = utils.scan_parquet(workplace_source, schema=WORKPLACE_SCHEMA).select(
            WORKPLACE_SCHEMA.keys()
        )

        lf = wUtils.apply_data_corrections(lf)

        lf = lf.filter(baseline_valid_workplace_filter())

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

        lf = lf.with_columns(
            pl.col(AWPClean.region_id).cast(CategoricalColumnTypes.RegionIdCatType),
            pl.col(AWPClean.establishment_type).cast(
                CategoricalColumnTypes.EstablishmentTypeCatType
            ),
            pl.col(AWPClean.registration_type).cast(
                CategoricalColumnTypes.RegistrationTypeCatType
            ),
            pl.col(AWPClean.main_service_id).cast(
                CategoricalColumnTypes.MainServiceIdCatType
            ),
            pl.col(AWPClean.la_permission).cast(
                CategoricalColumnTypes.LaPermissionCatType
            ),
            pl.col(AWPClean.is_bulk_uploader).cast(
                CategoricalColumnTypes.IsBulkUploaderCatType
            ),
            pl.col(AWPClean.is_parent).cast(CategoricalColumnTypes.IsParentCatType),
        )

        lf = wUtils.create_purge_date_columns(lf)

        diagnostics.checkpoint("before_sfc_internal_filter", lf)
        sfc_internal_lf = lf.filter(
            pl.col(AWPClean.workplace_last_active_date) >= pl.col(AWPClean.purge_date)
        ).select(SFC_INTERNAL_COLUMNS)

        diagnostics.checkpoint("before_sfc_internal_sink", sfc_internal_lf)
        utils.sink_to_parquet(
            sfc_internal_lf, output_path=ascwds_for_sfc_internal_destination
        )
        diagnostics.checkpoint("after_sfc_internal_sink")

        workplace_lf = lf.filter(
            pl.col(AWPClean.data_last_amended_date) >= pl.col(AWPClean.purge_date)
        )

        workplace_lf = wUtils.remove_rows_with_duplicate_location_ids(workplace_lf)

        diagnostics.checkpoint("before_combined_schema_scan")
        combined_schema = utils.discover_combined_schema(workplace_source)
        slv_lf = utils.scan_parquet(workplace_source, schema=combined_schema).select(
            *[AWPClean.establishment_id, AWPClean.import_date],
            expr.is_slv_job_role_column(),
        )

        slv_lf = slv_lf.drop(cs.starts_with("jr33"))

        slv_lf = slv_lf.with_columns(pl.col(AWPClean.import_date).cast(pl.String))

        workplace_lf = workplace_lf.join(
            slv_lf, on=[AWPClean.establishment_id, AWPClean.import_date], how="left"
        )

        workplace_lf = workplace_lf.with_columns(
            pl.col(INT_COLUMNS).cast(pl.Int32, strict=False),
            expr.is_slv_job_role_column().cast(pl.Int32, strict=False),
        )

        workplace_lf = workplace_lf.with_columns(
            bounds.filled_posts_expr,
            bounds.slv_expr,
        )

        workplace_lf = wUtils.merge_legacy_job_role_columns(
            workplace_lf, legacy_job_roles_dict
        )

        diagnostics.checkpoint("before_workplace_sink", workplace_lf)
        utils.sink_to_parquet(workplace_lf, output_path=cleaned_workplace_destination)
        diagnostics.checkpoint("after_workplace_sink")
    finally:
        diagnostics.stop()


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
