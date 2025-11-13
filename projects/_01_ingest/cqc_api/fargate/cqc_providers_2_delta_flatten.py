from polars_utils import logger, utils
from schemas.cqc_provider_schema_polars import POLARS_PROVIDER_SCHEMA
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cols_to_import = [
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


def main(delta_api_source: str, flattened_destination: str) -> None:
    cqc_lf = utils.scan_parquet(
        delta_api_source,
        schema=POLARS_PROVIDER_SCHEMA,
        selected_columns=cols_to_import,
    )

    utils.sink_to_parquet(
        cqc_lf,
        flattened_destination,
        logger=logger,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    logger.info("Running Flatten CQC providers job")

    args = utils.get_args(
        (
            "--delta_api_source",
            "S3 URI to read CQC providers raw API delta data from",
        ),
        (
            "--flattened_destination",
            "S3 URI to save flattened CQC providers data to",
        ),
    )

    main(
        delta_api_source=args.delta_api_source,
        flattened_destination=args.flattened_destination,
    )

    logger.info("Finished Flatten CQC providers job")
