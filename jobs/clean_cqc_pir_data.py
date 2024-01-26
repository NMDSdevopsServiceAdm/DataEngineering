import sys

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

pirPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(cqc_pir_source: str, cleaned_cqc_pir_destination: str):
    cqc_pir_df = utils.read_from_parquet(cqc_pir_source)
    utils.write_to_parquet(
        cqc_pir_df,
        cleaned_cqc_pir_destination,
        append=True,
        partitionKeys=pirPartitionKeys,
    )


if __name__ == "__main__":
    print("Spark job 'clean_cqc_pir_data' starting...")
    print(f"Job parameters: {sys.argv}")

    cqc_pir_source, cleaned_cqc_pir_destination = utils.collect_arguments(
        (
            "--cqc_pir_source",
            "Source s3 directory for parquet CQC providers dataset",
        ),
        (
            "--cleaned_cqc_pir_destination",
            "Destination s3 directory for cleaned parquet CQC providers dataset",
        ),
    )

    main(cqc_pir_source, cleaned_cqc_pir_destination)

    print("Spark job 'clean_cqc_pir_data' complete")
