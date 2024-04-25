from pyspark.sql import DataFrame

from utils import utils

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


def main(postcode_directory_source):
    # TODO: 1 - create a dataframe from the cleaned ons postcode directory.

    postcode_directory_df = utils.read_from_parquet(
        postcode_directory_source,
        [
            ONSClean.contemporary_ons_import_date,
            ONSClean.postcode,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ],
    )

    # TODO 2 - Create column with count of postcodes by LA.

    # TODO 3 - Create column with count of postcodes by hybrid area.

    # TODO 4 - Create column with ratio.

    # TODO 5 - Drop duplicates.

    # TODO 6 - Join pa filled posts.

    # TODO 7 - Apply ratio to calculate ICB filled posts.





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
