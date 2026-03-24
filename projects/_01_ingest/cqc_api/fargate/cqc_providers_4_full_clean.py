import polars as pl

from polars_utils import utils
from polars_utils.cleaning_utils import column_to_date
from schemas.cqc_provider_schema_polars import POLARS_PROVIDER_SCHEMA
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_values.categorical_column_values import RegistrationStatus

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_provider_cols_to_import = [
    CQCP.provider_id,
    CQCP.name,
    CQCP.brand_id,
    CQCP.brand_name,
    CQCP.type,
    CQCP.registration_status,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
]


def main(full_flattened_source: str, full_cleaned_destination: str) -> None:
    # Scan parquet to get CQC providers full data in LazyFrame format
    cqc_lf = utils.scan_parquet(
        full_flattened_source,
        schema=POLARS_PROVIDER_SCHEMA,
        selected_columns=cqc_provider_cols_to_import,
    )
    print("Full Flattened CQC Provider LazyFrame read in")

    cqc_reg_lf = cqc_lf.filter(
        pl.col(CQCPClean.registration_status) == RegistrationStatus.registered
    )

    cqc_reg_lf = column_to_date(
        cqc_reg_lf, Keys.import_date, CQCPClean.cqc_provider_import_date
    )

    utils.sink_to_parquet(
        cqc_reg_lf,
        full_cleaned_destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Clean Full CQC Providers job")

    args = utils.get_args(
        (
            "--full_flattened_source",
            "S3 URI to read CQC providers full flattened data from",
        ),
        (
            "--full_cleaned_destination",
            "S3 URI to save full cleaned CQC registered providers data to",
        ),
    )

    main(
        full_flattened_source=args.full_flattened_source,
        full_cleaned_destination=args.full_cleaned_destination,
    )

    print("Finished Clean Full CQC Providers job")
