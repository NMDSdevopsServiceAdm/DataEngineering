from pyspark.sql import (
    DataFrame,
    Window,
    functions as F,
)

from utils import utils

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)


def main(postcode_directory_source, pa_filled_posts_source, destination):
    postcode_directory_df = utils.read_from_parquet(
        postcode_directory_source,
        [
            ONSClean.contemporary_ons_import_date,
            ONSClean.postcode,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ],
    )

    pa_filled_posts_df = utils.read_from_parquet(
        pa_filled_posts_source,
        [
            DPColNames.LA_AREA,
            DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
            DPColNames.YEAR,
        ],
    )

    # TODO 1 - Create column with count of postcodes by LA.
    postcode_directory_df = count_postcodes_per_la(postcode_directory_df)

    # TODO 2 - Create column with count of postcodes by hybrid area.

    # TODO 3 - Create column with ratio.

    # TODO 4 - Drop duplicates.

    # TODO 5 - Join pa filled posts.

    # TODO 6 - Apply ratio to calculate ICB filled posts.

    utils.write_to_parquet(
        pa_filled_posts_df,
        destination,
        mode="overwrite",
        partitionKeys=[DPColNames.YEAR],
    )


def count_postcodes_per_la(postcode_directory_df: DataFrame) -> DataFrame:
    w = Window.partitionBy(
        ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr
    ).orderBy(ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr)

    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
        F.size(F.collect_set(ONSClean.postcode).over(w)),
    )

    return postcode_directory_df


def create_hybrid_area_column(postcode_directory_df: DataFrame) -> DataFrame:
    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.HYBRID_AREA_LA_ICB,
        F.concat(ONSClean.contemporary_cssr, F.lit(" - "), ONSClean.contemporary_icb),
    )

    return postcode_directory_df


def count_postcodes_per_hybrid_area(postcode_directory_df: DataFrame) -> DataFrame:
    postcode_directory_df = create_hybrid_area_column(postcode_directory_df)
    w = Window.partitionBy(
        ONSClean.contemporary_ons_import_date,
        DPColNames.HYBRID_AREA_LA_ICB,
    ).orderBy(
        ONSClean.contemporary_ons_import_date,
        DPColNames.HYBRID_AREA_LA_ICB,
    )

    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
        F.size(F.collect_set(ONSClean.postcode).over(w)),
    )

    postcode_directory_df.show()

    return postcode_directory_df


if __name__ == "__main__":
    (
        postcode_directory_source,
        pa_filled_posts_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--postcode_directory_source",
            "Source s3 directory for cleaned ons postcode directory",
        ),
        (
            "--pa_filled_posts_souce",
            "Source s3 directory for estimated pa filled posts split by LA area",
        ),
        (
            "--destination",
            "A destination directory for outputting pa filled posts split by ICB area",
        ),
    )

    main(postcode_directory_source, pa_filled_posts_source, destination)
