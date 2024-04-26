import sys
from pyspark.sql import DataFrame, functions as F

from utils import (
    utils,
    cleaning_utils as cUtils,
)

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
    PartitionKeys as Keys,
)
from utils.cqc_ratings_utils.cqc_ratings_values import (
    CQCRatingsColumns as CQCRatings,
    CQCRatingsValues,
)
from utils.value_labels.cqc_ratings.label_dictionary import (
    labels_dict as UnknownCurrentRatings,
)


cqc_location_columns = [
    CQCL.location_id,
    Keys.import_date,
    CQCL.current_ratings,
    CQCL.historic_ratings,
    CQCL.registration_status,
    CQCL.type,
]

ascwds_workplace_columns = [
    Keys.import_date,
    AWP.establishment_id,
    AWP.location_id,
]


def main(
    cqc_location_source: str,
    ascwds_workplace_source: str,
    cqc_ratings_destination: str,
    benchmark_ratings_destination: str,
):
    cqc_location_df = utils.read_from_parquet(cqc_location_source, cqc_location_columns)
    ascwds_workplace_df = utils.read_from_parquet(
        ascwds_workplace_source, ascwds_workplace_columns
    )

    cqc_location_df = filter_to_monthly_import_date(cqc_location_df)

    cqc_location_df = utils.select_rows_with_value(
        cqc_location_df, CQCL.type, CQCLValues.social_care_identifier
    )

    current_ratings_df = prepare_current_ratings(cqc_location_df)

    # prepare historic ratings
    # flatten
    # for each category
    # recode unknown ratings to null
    # join categories
    # add current/ histric column

    # join current and historic
    # remove blanks
    # add rating sequence column
    # Add latest rating flag
    # select columns for saving

    utils.write_to_parquet(
        current_ratings_df,
        cqc_ratings_destination,
        mode="overwrite",
    )

    # select ratings for benchmarks
    # create flag for good and outsatnding
    # add establishment ids
    # select rows and columns to save

    # save ratings for benchmarks


def filter_to_monthly_import_date(cqc_location_df: DataFrame) -> DataFrame:
    max_import_date = cqc_location_df.agg(
        F.max(cqc_location_df[Keys.import_date])
    ).collect()[0][0]
    first_day_of_the_month = "01"
    month_and_year_of_import_date = max_import_date[0:6]
    monthly_import_date = month_and_year_of_import_date + first_day_of_the_month
    cqc_location_df = cqc_location_df.where(
        cqc_location_df[Keys.import_date] == monthly_import_date
    )
    return cqc_location_df


def prepare_current_ratings(cqc_location_df: DataFrame) -> DataFrame:
    ratings_df = flatten_current_ratings(cqc_location_df)
    ratings_df = recode_unknown_codes_to_null(ratings_df)
    ratings_df = add_current_or_historic_column(ratings_df, CQCRatingsValues.current)
    return ratings_df


def prepare_historic_ratings(cqc_location_df: DataFrame) -> DataFrame:
    ratings_df = flatten_historic_ratings(cqc_location_df)
    # join categories
    ratings_df = recode_unknown_codes_to_null(ratings_df)
    ratings_df = add_current_or_historic_column(ratings_df, CQCRatingsValues.historic)
    return ratings_df


def flatten_current_ratings(cqc_location_df: DataFrame) -> DataFrame:
    current_ratings_df = cqc_location_df.select(
        cqc_location_df[CQCL.location_id],
        cqc_location_df[CQCL.registration_status],
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.report_date].alias(
            CQCRatings.date
        ),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.rating].alias(
            CQCRatings.overall_rating
        ),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.key_question_ratings][
            0
        ][CQCL.rating].alias(CQCRatings.safe_rating),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.key_question_ratings][
            1
        ][CQCL.rating].alias(CQCRatings.well_led_rating),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.key_question_ratings][
            2
        ][CQCL.rating].alias(CQCRatings.caring_rating),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.key_question_ratings][
            3
        ][CQCL.rating].alias(CQCRatings.responsive_rating),
        cqc_location_df[CQCL.current_ratings][CQCL.overall][CQCL.key_question_ratings][
            4
        ][CQCL.rating].alias(CQCRatings.effective_rating),
    )
    return current_ratings_df


def flatten_historic_ratings(cqc_location_df: DataFrame) -> DataFrame:
    historic_ratings_df = cqc_location_df.select(
        cqc_location_df[CQCL.location_id],
        cqc_location_df[CQCL.registration_status],
        F.explode(cqc_location_df[CQCL.historic_ratings]).alias(CQCL.historic_ratings),
    )
    historic_ratings_df = historic_ratings_df.select(
        historic_ratings_df[CQCL.location_id],
        historic_ratings_df[CQCL.registration_status],
        historic_ratings_df[CQCL.historic_ratings][CQCL.report_date].alias(
            CQCRatings.date
        ),
        historic_ratings_df[CQCL.historic_ratings][CQCL.overall][CQCL.rating].alias(
            CQCRatings.overall_rating
        ),
        F.explode(
            historic_ratings_df[CQCL.historic_ratings][CQCL.overall][
                CQCL.key_question_ratings
            ]
        ).alias(CQCL.key_question_ratings),
    )
    historic_ratings_df = historic_ratings_df.select(
        historic_ratings_df[CQCL.location_id],
        historic_ratings_df[CQCL.registration_status],
        historic_ratings_df[CQCRatings.date],
        historic_ratings_df[CQCRatings.overall_rating],
        historic_ratings_df[CQCL.key_question_ratings][CQCL.name].alias(CQCL.name),
        historic_ratings_df[CQCL.key_question_ratings][CQCL.rating].alias(CQCL.rating),
    )
    ratings_columns = ["Safe", "Well-led", "Caring", "Responsive", "Effective"]
    cleaned_historic_ratings_df = historic_ratings_df.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCRatings.overall_rating,
    ).dropDuplicates()
    for column in ratings_columns:
        column_name = column + "_rating"
        df = historic_ratings_df.where(historic_ratings_df[CQCL.name] == column)
        df = df.select(
            df[CQCL.location_id],
            df[CQCL.registration_status],
            df[CQCRatings.date],
            df[CQCRatings.overall_rating],
            df[CQCL.rating].alias(column_name),
        )
        cleaned_historic_ratings_df = cleaned_historic_ratings_df.join(
            df,
            [
                CQCL.location_id,
                CQCL.registration_status,
                CQCRatings.date,
                CQCRatings.overall_rating,
            ],
            "outer",
        )

    return cleaned_historic_ratings_df


def recode_unknown_codes_to_null(ratings_df: DataFrame) -> DataFrame:
    ratings_df = cUtils.apply_categorical_labels(
        ratings_df,
        UnknownCurrentRatings,
        UnknownCurrentRatings.keys(),
        add_as_new_column=False,
    )
    return ratings_df


def add_current_or_historic_column(
    ratings_df: DataFrame, current_or_historic: str
) -> DataFrame:
    ratings_df = ratings_df.withColumn(
        CQCRatings.current_or_historic, F.lit(current_or_historic)
    )
    return ratings_df


if __name__ == "__main__":
    print("Spark job 'flatten_cqc_ratings' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        ascwds_workplace_source,
        cqc_ratings_destination,
        benchmark_ratings_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace dataset",
        ),
        (
            "--cqc_ratings_destination",
            "Destination s3 directory for cleaned parquet CQC ratings dataset",
        ),
        (
            "--benchmark_ratings_destination",
            "Destination s3 directory for cleaned parquet benchmark ratings dataset",
        ),
    )
    main(
        cqc_location_source,
        ascwds_workplace_source,
        cqc_ratings_destination,
        benchmark_ratings_destination,
    )

    print("Spark job 'flatten_cqc_ratings' complete")
