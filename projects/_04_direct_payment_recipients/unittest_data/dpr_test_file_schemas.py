from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DateType,
    DoubleType,
)

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class PAFilledPostsByIcbAreaSchema:
    sample_ons_contemporary_with_duplicates_schema = StructType(
        [
            StructField(ONSClean.postcode, StringType(), True),
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
        ]
    )

    sample_ons_contemporary_schema = sample_ons_contemporary_with_duplicates_schema

    expected_postcode_count_per_la_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
        ]
    )

    expected_postcode_count_per_la_icb_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    sample_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
        ]
    )

    expected_ratio_between_hybrid_area_and_la_area_postcodes_schema = StructType(
        [
            *sample_rows_with_la_and_hybrid_area_postcode_counts_schema,
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    full_rows_with_la_and_hybrid_area_postcode_counts_schema = StructType(
        [
            *sample_ons_contemporary_schema,
            StructField(DP.COUNT_OF_DISTINCT_POSTCODES_PER_LA, IntegerType(), True),
            StructField(
                DP.COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA, IntegerType(), True
            ),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema = StructType(
        [
            StructField(ONSClean.contemporary_ons_import_date, DateType(), True),
            StructField(ONSClean.contemporary_cssr, StringType(), True),
            StructField(ONSClean.contemporary_icb, StringType(), True),
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
        ]
    )

    sample_pa_filled_posts_schema = StructType(
        [
            StructField(DP.LA_AREA, StringType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    expected_create_date_column_from_year_in_pa_estimates_schema = StructType(
        [
            *sample_pa_filled_posts_schema,
            StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
        ]
    )

    sample_postcode_proportions_before_joining_pa_filled_posts_schema = (
        expected_deduplicated_import_date_hybrid_and_la_and_ratio_schema
    )

    sample_pa_filled_posts_prepared_for_joining_to_postcode_proportions_schema = (
        StructType(
            [
                StructField(DP.LA_AREA, StringType(), True),
                StructField(
                    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
                    DoubleType(),
                    True,
                ),
                StructField(DP.YEAR, StringType(), True),
                StructField(DP.ESTIMATE_PERIOD_AS_DATE, DateType(), True),
            ]
        )
    )

    expected_postcode_proportions_after_joining_pa_filled_posts_schema = StructType(
        [
            *sample_postcode_proportions_before_joining_pa_filled_posts_schema,
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
            StructField(DP.YEAR, StringType(), True),
        ]
    )

    sample_proportions_and_pa_filled_posts_schema = StructType(
        [
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS, DoubleType(), True
            ),
        ]
    )

    expected_pa_filled_posts_after_applying_proportions_schema = StructType(
        [
            StructField(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA, FloatType(), True),
            StructField(
                DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA,
                DoubleType(),
                True,
            ),
        ]
    )

    sample_la_name_schema = StructType([StructField(DP.LA_AREA, StringType(), True)])

    expected_la_names_with_correct_spelling_schema = sample_la_name_schema


@dataclass
class CalculatePaRatioSchemas:
    total_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.TOTAL_STAFF_RECODED,
                FloatType(),
                True,
            ),
        ]
    )
    average_staff_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.AVERAGE_STAFF,
                FloatType(),
                True,
            ),
        ]
    )
    reduce_year_by_one_schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField("other column", StringType(), True),
        ]
    )
