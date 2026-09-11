import sys

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from projects._01_ingest.cqc_pir.fargate.utils import (
    clean_cqc_pir_utils as cpUtils,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_values.categorical_column_values import PIRType

PIR_SUBMISSION_DATE_FORMAT = "%d-%b-%y"

CQC_PIR_SCHEMA = pl.Schema(
    [
        (PIRCols.location_id, pl.String),
        (PIRCols.location_name, pl.String),
        (PIRCols.pir_type, pl.String),
        (PIRCols.pir_submission_date, pl.String),
        (PIRCols.pir_people_directly_employed, pl.Int32),
        (PIRCols.staff_leavers, pl.Int32),
        (PIRCols.staff_vacancies, pl.Int32),
        (PIRCols.shared_lives_leavers, pl.Int32),
        (PIRCols.shared_lives_vacancies, pl.Int32),
        (PIRCols.primary_inspection_category, pl.String),
        (PIRCols.region, pl.String),
        (PIRCols.local_authority, pl.String),
        (PIRCols.number_of_beds, pl.Int32),
        (PIRCols.domiciliary_care, pl.String),
        (PIRCols.location_status, pl.String),
        (Keys.import_date, pl.String),
        (Keys.year, pl.String),
        (Keys.month, pl.String),
        (Keys.day, pl.String),
    ]
)


def main(cqc_pir_source: str, cleaned_cqc_pir_destination: str) -> None:
    """Cleans the raw CQC PIR dataset.

    Filters to locations that directly employ staff and submitted a
    residential or community PIR return, keeps only the latest submission per
    location/import date/care home grouping, and nulls out headcounts from
    locations that only submitted once but reported a large (>=100) figure.

    Args:
        cqc_pir_source (str): source s3 directory for parquet CQC PIR dataset.
        cleaned_cqc_pir_destination (str): destination s3 directory for
            cleaned parquet CQC PIR dataset.
    """
    cqc_pir_lf = utils.scan_parquet(cqc_pir_source, schema=CQC_PIR_SCHEMA)

    cqc_pir_lf = cqc_pir_lf.filter(
        (pl.col(PIRCols.pir_people_directly_employed) > 0)
        & pl.col(PIRCols.pir_type).is_in([PIRType.residential, PIRType.community])
    )

    cqc_pir_lf = cUtils.column_to_date(
        cqc_pir_lf, Keys.import_date, PIRClean.cqc_pir_import_date
    ).drop(Keys.year, Keys.month, Keys.day, Keys.import_date)

    cqc_pir_lf = cUtils.column_to_date(
        cqc_pir_lf,
        PIRClean.pir_submission_date,
        PIRClean.pir_submission_date_as_date,
        format=PIR_SUBMISSION_DATE_FORMAT,
    )

    cqc_pir_lf = cpUtils.add_care_home_column(cqc_pir_lf)

    cqc_pir_lf = cpUtils.filter_latest_submission_date(cqc_pir_lf)

    cqc_pir_lf = cpUtils.null_people_directly_employed_outliers(cqc_pir_lf)

    # Cast to Categorical/Enum here so it's saved in the output parquet file.
    cqc_pir_lf = cqc_pir_lf.with_columns(
        pl.col(PIRClean.care_home).cast(CategoricalColumnTypes.CareHomeEnumType)
    )

    utils.sink_to_parquet(cqc_pir_lf, cleaned_cqc_pir_destination)


if __name__ == "__main__":
    print(f"Fargate job 'clean_cqc_pir_data' called with parameters: {sys.argv}")

    args = utils.get_args(
        (
            "--cqc_pir_source",
            "Source s3 directory for parquet CQC PIR dataset",
        ),
        (
            "--cleaned_cqc_pir_destination",
            "Destination s3 directory for cleaned parquet CQC PIR dataset",
        ),
    )

    main(args.cqc_pir_source, args.cleaned_cqc_pir_destination)
    print("Fargate job 'clean_cqc_pir_data' complete")
