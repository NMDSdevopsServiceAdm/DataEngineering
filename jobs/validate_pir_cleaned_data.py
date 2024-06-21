import os
import sys

os.environ["SPARK_VERSION"] = "3.3"

from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.pir_cleaned_validation_rules import (
    PIRCleanedValidationRules as Rules,
)
from utils.validation.validation_utils import (
    validate_dataset,
    raise_exception_if_any_checks_failed,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_cqc_pir_source: str,
    report_destination: str,
):
    cleaned_cqc_pir_df = utils.read_from_parquet(
        cleaned_cqc_pir_source,
    )
    rules = Rules.rules_to_check

    check_result_df = validate_dataset(cleaned_cqc_pir_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")

    if isinstance(check_result_df, DataFrame):
        raise_exception_if_any_checks_failed(check_result_df)


if __name__ == "__main__":
    print("Spark job 'validate_pir_api_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_pir_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_pir_source",
            "Source s3 directory for parquet pir api cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            cleaned_cqc_pir_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_pir_api_cleaned_data' complete")
