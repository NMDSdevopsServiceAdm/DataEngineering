import sys

import pointblank as pb

from polars_utils import utils
from polars_utils import validate as vl
from polars_utils.logger import get_logger
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.validation_table_columns import (
    Validation,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationsApiCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_utils import (
    validate_dataset,
)

logger = get_logger(__name__)

GLOBAL_THRESHOLDS = pb.Thresholds(warning=1)

COLS_NOT_NULL = [CQCL.location_id, Keys.import_date, CQCL.name]
ROWS_DISTINCT = [CQCL.location_id, Keys.import_date]

COMPLETE_COLUMNS = [
    CQCLClean.location_id,
    CQCLClean.cqc_location_import_date,
    CQCLClean.care_home,
    CQCLClean.provider_id,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.primary_service_type,
    CQCLClean.name,
    CQCLClean.contemporary_ons_import_date,
    CQCLClean.contemporary_cssr,
    CQCLClean.contemporary_region,
    CQCLClean.current_ons_import_date,
    CQCLClean.current_cssr,
    CQCLClean.current_region,
    CQCLClean.current_rural_urban_ind_11,
]

INDEX_COLUMNS = (
    [
        CQCLClean.location_id,
        CQCLClean.cqc_location_import_date,
    ],
)
MIN_VALUES = (
    {
        CQCLClean.number_of_beds: 0,
        CQCLClean.time_registered: 1,
        Validation.location_id_length: 3,
        Validation.provider_id_length: 3,
    },
)
MAX_VALUES = (
    {
        CQCLClean.number_of_beds: 500,
        Validation.location_id_length: 14,
        Validation.provider_id_length: 14,
    },
)
CATEGORICAL_VALUES_IN_COLUMNS = (
    {
        CQCLClean.care_home: CatValues.care_home_column_values.categorical_values,
        CQCLClean.cqc_sector: CatValues.sector_column_values.categorical_values,
        CQCLClean.registration_status: CatValues.registration_status_column_values.categorical_values,
        CQCLClean.dormancy: CatValues.dormancy_column_values.categorical_values,
        CQCLClean.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
        CQCLClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.categorical_values,
        CQCLClean.contemporary_region: CatValues.contemporary_region_column_values.categorical_values,
        CQCLClean.current_cssr: CatValues.current_cssr_column_values.categorical_values,
        CQCLClean.current_region: CatValues.current_region_column_values.categorical_values,
        CQCLClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.categorical_values,
        CQCLClean.related_location: CatValues.related_location_column_values.categorical_values,
    },
)
DISTINCT_VALUES = {
    CQCLClean.care_home: CatValues.care_home_column_values.count_of_categorical_values,
    CQCLClean.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
    CQCLClean.registration_status: CatValues.registration_status_column_values.count_of_categorical_values,
    CQCLClean.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
    CQCLClean.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
    CQCLClean.contemporary_cssr: CatValues.contemporary_cssr_column_values.count_of_categorical_values,
    CQCLClean.contemporary_region: CatValues.contemporary_region_column_values.count_of_categorical_values,
    CQCLClean.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
    CQCLClean.current_region: CatValues.current_region_column_values.count_of_categorical_values,
    CQCLClean.current_rural_urban_ind_11: CatValues.current_rui_column_values.count_of_categorical_values,
    CQCLClean.related_location: CatValues.related_location_column_values.count_of_categorical_values,
}


def validate_dataset(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = vl.read_parquet(f"""s3://{bucket_name}/{source_path.strip("/")}/""")

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
        )
        .col_vals_not_null(COLS_NOT_NULL)
        .rows_distinct(ROWS_DISTINCT)
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path.strip("/"))


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    logger.info(f"Starting validation for {args.dataset}")

    validate_dataset(args.bucket_name, args.source_path, args.report_path)
    logger.info(f"Validation of {args.dataset} complete")
