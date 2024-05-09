from pyspark.sql import (
    DataFrame,
    Window,
    functions as F,
)

from datetime import date

from utils import utils, cleaning_utils

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
    postcode_directory_df = count_postcodes_per_list_of_columns(
        postcode_directory_df,
        [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
    )

    # TODO 2 - Create column with count of postcodes by hybrid area.
    postcode_directory_df = count_postcodes_per_list_of_columns(
        postcode_directory_df,
        [
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ],
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
    )

    # TODO 3 - Create column with ratio.
    postcode_directory_df = (
        create_ratio_between_hybrid_area_and_la_area_postcode_counts(
            postcode_directory_df,
        )
    )

    # TODO 4 - Drop duplicates.
    postcode_directory_df = (
        deduplicate_ratio_between_hybrid_area_and_la_area_postcode_counts(
            postcode_directory_df
        )
    )

    # TODO 5 - Join pa filled posts.

    # TODO 6 - Apply ratio to calculate ICB filled posts.

    utils.write_to_parquet(
        pa_filled_posts_df,
        destination,
        mode="overwrite",
        partitionKeys=[DPColNames.YEAR],
    )


def count_postcodes_per_list_of_columns(
    postcode_directory_df: DataFrame,
    list_of_columns_to_group_by: list,
    new_column_name: str,
) -> DataFrame:
    w = Window.partitionBy(list_of_columns_to_group_by).orderBy(
        list_of_columns_to_group_by
    )

    postcode_directory_df = postcode_directory_df.withColumn(
        new_column_name,
        F.size(F.collect_set(ONSClean.postcode).over(w)),
    )

    return postcode_directory_df


def create_ratio_between_hybrid_area_and_la_area_postcode_counts(
    postcode_directory_df: DataFrame,
) -> DataFrame:
    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA,
        F.col(DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA)
        / F.col(DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA),
    )

    return postcode_directory_df


def deduplicate_ratio_between_hybrid_area_and_la_area_postcode_counts(
    postcode_directory_df: DataFrame,
) -> DataFrame:
    postcode_directory_df = postcode_directory_df.drop(
        ONSClean.postcode,
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
    )

    postcode_directory_df = postcode_directory_df.distinct()

    return postcode_directory_df


def join_pa_filled_posts_to_hybrid_area_proportions_table(
    postcode_directory_df: DataFrame,
    pa_filled_posts_df: DataFrame,
) -> DataFrame:
    pa_filled_posts_df = pa_filled_posts_df.withColumn(
        DPColNames.ESTIMATE_PERIOD_AS_DATE, date(DPColNames.YEAR, 3, 31)
    )

    pa_filled_posts_df = cleaning_utils.add_aligned_date_column(
        pa_filled_posts_df,
        postcode_directory_df,
        DPColNames.ESTIMATE_PERIOD_AS_DATE,
        ONSClean.contemporary_ons_import_date,
    )


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
