import sys

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


from utils import utils
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ColNames,
)

POSTCODE_DIR_PREFIX = "dataset=postcode-directory"
POSTCODE_LOOKUP_DIR_PREFIX = "dataset=postcode-directory-field-lookups"


def main(source, destination):
    spark = utils.get_spark()

    





if __name__ == "__main__":
    print("Spark job 'inges_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    ons_source, ons_destination = utils.collect_arguments(
        ("--source", "S3 path to the ONS raw data domain"),
        ("--destination", "S3 path to save output data"),
    )
    main(ons_source, ons_destination)
