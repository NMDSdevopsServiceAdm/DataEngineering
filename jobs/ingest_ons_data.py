import sys

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


from utils import utils
from utils.ind_cqc_column_names.ons_columns import (
    OnsPostcodeDirectoryColumns as ColNames,
)

POSTCODE_DIR_PREFIX = "dataset=postcode-directory"
POSTCODE_LOOKUP_DIR_PREFIX = "dataset=postcode-directory-field-lookups"


def main(source, destination):
    spark = utils.get_spark()

    ingest_new_import_dates(
        spark, f"{source}{POSTCODE_DIR_PREFIX}/", f"{destination}{POSTCODE_DIR_PREFIX}/"
    )
    all_fields = utils.get_s3_sub_folders_for_path(
        f"{source}{POSTCODE_LOOKUP_DIR_PREFIX}/"
    )

    print(
        f"found lookup tables for {all_fields} in {source}{POSTCODE_LOOKUP_DIR_PREFIX}"
    )

    for field in all_fields:
        print(f"Ingesting lookup table for {field}")
        ingest_new_import_dates(
            spark,
            f"{source}{POSTCODE_LOOKUP_DIR_PREFIX}/{field}/",
            f"{destination}{POSTCODE_LOOKUP_DIR_PREFIX}/{field}/",
        )

    return


def ingest_new_import_dates(spark, source, destination):
    print(f"Reading CSV from {source}")
    data_to_import = spark.read.option("header", True).csv(source)
    previously_imported_dates = get_previous_import_dates(spark, destination)

    if previously_imported_dates:
        data_to_import = data_to_import.join(
            previously_imported_dates,
            data_to_import.import_date
            == previously_imported_dates.already_imported_date,
            "leftouter",
        ).where(previously_imported_dates.already_imported_date.isNull())
    print(f"Writing CSV to {destination}")
    data_to_import.write.mode("append").partitionBy(
        ColNames.year, ColNames.month, ColNames.day, ColNames.import_date
    ).parquet(destination)
    return data_to_import


def get_previous_import_dates(spark, destination):
    try:
        df = spark.read.parquet(destination)
    except AnalysisException:
        return None

    return df.select(
        F.col(ColNames.import_date).alias("already_imported_date")
    ).distinct()


if __name__ == "__main__":
    print("Spark job 'inges_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    ons_source, ons_destination = utils.collect_arguments(
        ("--source", "S3 path to the ONS raw data domain"),
        ("--destination", "S3 path to save output data"),
    )
    main(ons_source, ons_destination)
