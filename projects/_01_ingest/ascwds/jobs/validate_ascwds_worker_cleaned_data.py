import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from utils import utils
from utils.column_values.categorical_columns_by_dataset import (
    ASCWDSWorkerCleanedCategoricalValues as CatValues,
)
from utils.validation.validation_rules.ascwds_worker_cleaned_validation_rules import (
    ASCWDSWorkerCleanedValidationRules as Rules,
)
from utils.validation.validation_utils import validate_dataset


def main(cleaned_ascwds_worker_source: str, report_destination: str):
    cleaned_ascwds_worker_df = utils.read_from_parquet(cleaned_ascwds_worker_source)

    # count distinct values in job role column.
    print(
        cleaned_ascwds_worker_df.select(
            "mainjrid_clean_labels",
        ).distinct()
    )

    print(
        cleaned_ascwds_worker_df.select(
            "mainjrid_clean_labels",
        )
        .distinct()
        .count()
    )

    print(CatValues.main_job_role_labels_column_values.count_of_categorical_values)

    rules = Rules.rules_to_check

    check_result_df = validate_dataset(cleaned_ascwds_worker_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


if __name__ == "__main__":
    print("Spark job 'validate_ascwds_worker_cleaned_data' starting...")
    print(f"Job parameters: {sys.argv}")

    cleaned_ascwds_worker_source, report_destination = utils.collect_arguments(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for parquet ascwds worker api cleaned dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(cleaned_ascwds_worker_source, report_destination)
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_ascwds_worker_cleaned_data' complete")
