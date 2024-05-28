import os
import sys

os.environ["SPARK_VERSION"] = "3.3"


from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rules.ascwds_worker_raw_validation_rules import (
    ASCWDSWorkerRawValidationRules as Rules,
)
from utils.validation.validation_utils import validate_dataset

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    raw_ascwds_worker_source: str,
    report_destination: str,
):
    raw_ascwds_worker_df = utils.read_from_parquet(
        raw_ascwds_worker_source,
    )
    rules = Rules.rules_to_check

    check_result_df = validate_dataset(raw_ascwds_worker_df, rules)

    utils.write_to_parquet(check_result_df, report_destination, mode="overwrite")


if __name__ == "__main__":
    print("Spark job 'validate_ascwds_worker_raw_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        raw_ascwds_worker_source,
        report_destination,
    ) = utils.collect_arguments(
        (
            "--raw_ascwds_worker_source",
            "Source s3 directory for parquet ascwds worker raw dataset",
        ),
        (
            "--report_destination",
            "Destination s3 directory for validation report parquet",
        ),
    )
    try:
        main(
            raw_ascwds_worker_source,
            report_destination,
        )
    finally:
        spark = utils.get_spark()
        if spark.sparkContext._gateway:
            spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    print("Spark job 'validate_ascwds_worker_raw_data' complete")
