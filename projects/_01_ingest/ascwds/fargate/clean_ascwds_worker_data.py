import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.raw_data_adjustments import is_unique_worker_data
from projects._01_ingest.ascwds.fargate.utils import clean_worker_utils as wUtils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC

WORKER_SCHEMA = {
    AWKClean.location_id: pl.String,
    AWKClean.establishment_id: pl.String,
    AWKClean.worker_id: pl.String,
    AWKClean.main_job_role_id: pl.String,
    AWKClean.import_date: pl.String,
}

WORKPLACE_SCHEMA = {
    AWPClean.import_date: pl.String,
    AWPClean.establishment_id: pl.String,
}

data_labels_schema = pl.Schema(
    [(DLC.column_name, pl.String), (DLC.code, pl.String), (DLC.label, pl.String)]
)


def main(
    worker_source: str,
    cleaned_workplace_source: str,
    data_labels_source: str,
    cleaned_worker_destination: str,
) -> None:
    """
    Clean raw ASC-WDS worker data.

    Args:
        worker_source (str): path to the raw ASC-WDS worker data
        cleaned_workplace_source (str): path to the cleaned ASC-WDS workplace
            data - used to drop workers whose workplace was removed during
            workplace cleaning
        data_labels_source (str): path to the ASC-WDS data labels source
        cleaned_worker_destination (str): destination for cleaned ASC-WDS
            worker output
    """
    worker_lf = utils.scan_parquet(worker_source, schema=WORKER_SCHEMA).select(
        WORKER_SCHEMA.keys()
    )

    worker_lf = cUtils.column_to_date(
        worker_lf, AWKClean.import_date, AWKClean.ascwds_worker_import_date
    )

    worker_lf = worker_lf.filter(is_unique_worker_data())

    workplace_lf = utils.scan_parquet(
        cleaned_workplace_source, schema=WORKPLACE_SCHEMA
    ).select(WORKPLACE_SCHEMA.keys())

    worker_lf = wUtils.remove_workers_without_workplaces(worker_lf, workplace_lf)
    worker_lf = worker_lf.drop(AWKClean.import_date)

    data_labels_lf = pl.scan_csv(data_labels_source, schema=data_labels_schema)

    worker_lf = wUtils.create_clean_main_job_role_column(worker_lf, data_labels_lf)

    # Cast to Enum here so it's saved in the output parquet file.
    worker_lf = worker_lf.with_columns(
        pl.col(AWKClean.main_job_role_clean).cast(
            CategoricalColumnTypes.MainJobRoleIdEnumType
        ),
        pl.col(AWKClean.main_job_role_clean_labelled).cast(
            CategoricalColumnTypes.MainJobRoleLabelEnumType
        ),
    )

    utils.sink_to_parquet(worker_lf, output_path=cleaned_worker_destination)


if __name__ == "__main__":
    args = utils.get_args(
        ("--worker_source", "Source s3 directory for raw ASC-WDS worker data"),
        (
            "--cleaned_workplace_source",
            "Source s3 directory for cleaned ASC-WDS workplace data",
        ),
        ("--data_labels_source", "Source s3 directory for ASC-WDS data labels"),
        (
            "--cleaned_worker_destination",
            "Destination s3 directory for cleaned ASC-WDS worker output",
        ),
    )
    main(
        worker_source=args.worker_source,
        cleaned_workplace_source=args.cleaned_workplace_source,
        data_labels_source=args.data_labels_source,
        cleaned_worker_destination=args.cleaned_worker_destination,
    )
