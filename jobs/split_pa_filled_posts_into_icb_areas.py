from pyspark.sql import DataFrame

from utils import utils

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


def main(postcode_directory_source):

    postcode_directory_df = utils.read_from_parquet(
        postcode_directory_source,
        [
            ONSClean.contemporary_ons_import_date,
            ONSClean.postcode,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ],
    )

    # TODO 1 - Create column with count of postcodes by LA.

    # TODO 2 - Create column with count of postcodes by hybrid area.

    # TODO 3 - Create column with ratio.

    # TODO 4 - Drop duplicates.

    # TODO 5 - Join pa filled posts.

    # TODO 6 - Apply ratio to calculate ICB filled posts.





if __name__ == "__main__":
    (postcode_directory_source,) = utils.collect_arguments(
        (
            "--postcode_directory_source",
            "Source s3 directory for cleaned ons postcode directory",
        ),
    )

    main(
        postcode_directory_source,
    )
