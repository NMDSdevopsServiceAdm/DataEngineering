from pyspark.sql import (
    DataFrame,
    Window,
    functions as F,
)

from datetime import date

from utils import utils, cleaning_utils

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    EstimatePeriodAsDate,
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

    postcode_directory_df = count_postcodes_per_list_of_columns(
        postcode_directory_df,
        [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA,
    )

    postcode_directory_df = count_postcodes_per_list_of_columns(
        postcode_directory_df,
        [
            ONSClean.contemporary_ons_import_date,
            ONSClean.contemporary_cssr,
            ONSClean.contemporary_icb,
        ],
        DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA,
    )

    postcode_directory_df = (
        create_proportion_between_hybrid_area_and_la_area_postcode_counts(
            postcode_directory_df,
        )
    )

    proportion_of_postcodes_per_hybrid_area_df = (
        deduplicate_proportion_between_hybrid_area_and_la_area_postcode_counts(
            postcode_directory_df
        )
    )

    pa_filled_posts_df = create_date_column_from_year_in_pa_estimates(
        pa_filled_posts_df
    )

    proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df = (
        join_pa_filled_posts_to_hybrid_area_proportions(
            proportion_of_postcodes_per_hybrid_area_df, pa_filled_posts_df
        )
    )

    proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df = (
        apply_icb_proportions_to_pa_filled_posts(
            proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df
        )
    )

    utils.write_to_parquet(
        proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df,
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
        F.approx_count_distinct(ONSClean.postcode).over(w),
    )

    return postcode_directory_df


def create_proportion_between_hybrid_area_and_la_area_postcode_counts(
    postcode_directory_df: DataFrame,
) -> DataFrame:
    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA,
        F.col(DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA)
        / F.col(DPColNames.COUNT_OF_DISTINCT_POSTCODES_PER_LA),
    )

    return postcode_directory_df


def deduplicate_proportion_between_hybrid_area_and_la_area_postcode_counts(
    postcode_directory_df: DataFrame,
) -> DataFrame:
    proportion_of_postcodes_per_hybrid_area_df = postcode_directory_df.select(
        ONSClean.contemporary_ons_import_date,
        ONSClean.contemporary_cssr,
        ONSClean.contemporary_icb,
        DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA,
    ).distinct()

    return proportion_of_postcodes_per_hybrid_area_df


def create_date_column_from_year_in_pa_estimates(
    pa_filled_posts_df: DataFrame,
) -> DataFrame:
    pa_filled_posts_df = pa_filled_posts_df.withColumn(
        DPColNames.ESTIMATE_PERIOD_AS_DATE,
        F.to_date(
            F.concat(
                F.col(DPColNames.YEAR),
                F.lit(f"-{EstimatePeriodAsDate.MONTH}-{EstimatePeriodAsDate.DAY}"),
            )
        ),
    )

    return pa_filled_posts_df


def join_pa_filled_posts_to_hybrid_area_proportions(
    postcode_directory_df: DataFrame,
    pa_filled_posts_df: DataFrame,
) -> DataFrame:
    pa_filled_posts_df = cleaning_utils.add_aligned_date_column(
        pa_filled_posts_df,
        postcode_directory_df,
        DPColNames.ESTIMATE_PERIOD_AS_DATE,
        ONSClean.contemporary_ons_import_date,
    )

    pa_filled_posts_df = pa_filled_posts_df.select(
        DPColNames.LA_AREA,
        DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
        DPColNames.YEAR,
        ONSClean.contemporary_ons_import_date,
    )

    pa_filled_posts_df = pa_filled_posts_df.withColumnRenamed(
        DPColNames.LA_AREA, ONSClean.contemporary_cssr
    )

    proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df = (
        postcode_directory_df.join(
            pa_filled_posts_df,
            [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
            "left",
        )
    )

    return proportion_of_postcodes_per_hybrid_area_with_pa_filled_posts_df


def apply_icb_proportions_to_pa_filled_posts(
    postcode_directory_df: DataFrame,
) -> DataFrame:
    postcode_directory_df = postcode_directory_df.withColumn(
        DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_ICB,
        F.col(DPColNames.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS)
        * F.col(DPColNames.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA),
    )

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
