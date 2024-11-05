import sys

from pyspark.sql import (
    DataFrame,
    functions as F,
    Window,
)

from utils import (
    utils,
    cleaning_utils as cUtils,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cqc_ratings_columns import (
    CQCRatingsColumns as CQCRatings,
)
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
    CQCRatingsValues,
    CQCCurrentOrHistoricValues,
)
from utils.value_labels.cqc_ratings.label_dictionary import (
    unknown_ratings_labels_dict as UnknownRatings,
)

cqc_location_columns = [
    CQCL.location_id,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
    CQCL.current_ratings,
    CQCL.historic_ratings,
    CQCL.registration_status,
    CQCL.type,
]

ascwds_workplace_columns = [
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
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

    cqc_location_df = filter_to_first_import_of_most_recent_month(cqc_location_df)
    ascwds_workplace_df = filter_to_first_import_of_most_recent_month(
        ascwds_workplace_df
    )

    cqc_location_df = utils.select_rows_with_value(
        cqc_location_df, CQCL.type, LocationType.social_care_identifier
    )

    current_ratings_df = prepare_current_ratings(cqc_location_df)
    historic_ratings_df = prepare_historic_ratings(cqc_location_df)
    ratings_df = current_ratings_df.unionByName(historic_ratings_df)
    ratings_df = remove_blank_and_duplicate_rows(ratings_df)
    ratings_df = add_rating_sequence_column(ratings_df)
    ratings_df = add_rating_sequence_column(ratings_df, reversed=True)
    ratings_df = add_latest_rating_flag_column(ratings_df)
    ratings_df = add_numerical_ratings(ratings_df)
    standard_ratings_df = create_standard_ratings_dataset(ratings_df)
    standard_ratings_df = add_location_id_hash(standard_ratings_df)

    benchmark_ratings_df = select_ratings_for_benchmarks(ratings_df)
    benchmark_ratings_df = add_good_and_outstanding_flag_column(benchmark_ratings_df)
    benchmark_ratings_df = join_establishment_ids(
        benchmark_ratings_df, ascwds_workplace_df
    )
    benchmark_ratings_df = create_benchmark_ratings_dataset(benchmark_ratings_df)

    utils.write_to_parquet(
        standard_ratings_df,
        cqc_ratings_destination,
        mode="overwrite",
    )

    utils.write_to_parquet(
        benchmark_ratings_df,
        benchmark_ratings_destination,
        mode="overwrite",
    )


def filter_to_first_import_of_most_recent_month(df: DataFrame) -> DataFrame:
    max_year = df.agg(F.max(df[Keys.year])).collect()[0][0]
    df = df.where(df[Keys.year] == max_year)
    max_month = df.agg(F.max(df[Keys.month])).collect()[0][0]
    df = df.where(df[Keys.month] == max_month)
    min_day = df.agg(F.min(df[Keys.day])).collect()[0][0]
    df = df.where(df[Keys.day] == min_day)
    return df


def prepare_current_ratings(cqc_location_df: DataFrame) -> DataFrame:
    ratings_df = flatten_current_ratings(cqc_location_df)
    ratings_df = recode_unknown_codes_to_null(ratings_df)
    ratings_df = add_current_or_historic_column(
        ratings_df, CQCCurrentOrHistoricValues.current
    )
    return ratings_df


def prepare_historic_ratings(cqc_location_df: DataFrame) -> DataFrame:
    ratings_df = flatten_historic_ratings(cqc_location_df)
    ratings_df = recode_unknown_codes_to_null(ratings_df)
    ratings_df = add_current_or_historic_column(
        ratings_df, CQCCurrentOrHistoricValues.historic
    )
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
        UnknownRatings,
        UnknownRatings.keys(),
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


def remove_blank_and_duplicate_rows(ratings_df: DataFrame) -> DataFrame:
    ratings_df = ratings_df.where(
        (ratings_df[CQCRatings.overall_rating].isNotNull())
        | (ratings_df[CQCRatings.safe_rating].isNotNull())
        | (ratings_df[CQCRatings.well_led_rating].isNotNull())
        | (ratings_df[CQCRatings.caring_rating].isNotNull())
        | (ratings_df[CQCRatings.responsive_rating].isNotNull())
        | (ratings_df[CQCRatings.effective_rating].isNotNull())
    ).distinct()
    return ratings_df


def add_rating_sequence_column(ratings_df: DataFrame, reversed=False) -> DataFrame:
    if reversed == True:
        window = Window.partitionBy(CQCL.location_id).orderBy(F.desc(CQCRatings.date))
        new_column_name = CQCRatings.reversed_rating_sequence
    else:
        window = Window.partitionBy(CQCL.location_id).orderBy(F.asc(CQCRatings.date))
        new_column_name = CQCRatings.rating_sequence
    ratings_df = ratings_df.withColumn(new_column_name, F.rank().over(window))
    return ratings_df


def add_latest_rating_flag_column(ratings_df: DataFrame) -> DataFrame:
    ratings_df = ratings_df.withColumn(
        CQCRatings.latest_rating_flag,
        F.when(ratings_df[CQCRatings.reversed_rating_sequence] == 1, 1).otherwise(0),
    )
    return ratings_df


def add_numerical_ratings(df: DataFrame) -> DataFrame:
    """
    Adds numerical ratings columns for each of the key ratings and a total column.

    Args:
        df (DataFrame): A dataframe with flattened CQC key ratings columns.

    Returns:
        DataFrame: The given data frame with additional columns containing the key ratings as numerical values and a total of all the values.
    """
    rating_columns_dict = {
        CQCRatings.safe_rating: CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating: CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating: CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating: CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating: CQCRatings.effective_rating_value,
    }
    for rating_column, new_column_name in rating_columns_dict.items():
        df = df.withColumn(
            new_column_name,
            F.when(F.col(rating_column) == CQCRatingsValues.outstanding, F.lit(4))
            .when(F.col(rating_column) == CQCRatingsValues.good, F.lit(3))
            .when(
                F.col(rating_column) == CQCRatingsValues.requires_improvement, F.lit(2)
            )
            .when(F.col(rating_column) == CQCRatingsValues.inadequate, F.lit(1))
            .otherwise(F.lit(0)),
        )
    df = df.withColumn(
        CQCRatings.total_rating_value,
        (
            F.col(CQCRatings.safe_rating_value)
            + F.col(CQCRatings.well_led_rating_value)
            + F.col(CQCRatings.caring_rating_value)
            + F.col(CQCRatings.responsive_rating_value)
            + F.col(CQCRatings.effective_rating_value)
        ),
    )
    return df


def create_standard_ratings_dataset(ratings_df: DataFrame) -> DataFrame:
    standard_ratings_df = ratings_df.select(
        CQCL.location_id,
        CQCRatings.date,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
        CQCRatings.rating_sequence,
        CQCRatings.latest_rating_flag,
        CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating_value,
        CQCRatings.total_rating_value,
    ).distinct()
    return standard_ratings_df


def add_location_id_hash(df: DataFrame) -> DataFrame:
    """
    Adds a column with a 20 character hashed version of the location ID.

    Adds a column with a 20 character hashed version of the location ID. This hash is used for linking with anonymised files.

    Args:
        df(DataFrame): A prepared standard ratings dataframe containing the column location_id.

    Returns:
        DataFrame: The same dataframe with an additional column containing the hashed location id.
    """
    df = df.withColumn(CQCRatings.location_id_hash, F.sha2(df[CQCL.location_id], 256))
    df = df.withColumn(
        CQCRatings.location_id_hash, df[CQCRatings.location_id_hash].substr(1, 20)
    )
    return df


def select_ratings_for_benchmarks(ratings_df: DataFrame) -> DataFrame:
    benchmark_ratings_df = ratings_df.where(
        (ratings_df[CQCL.registration_status] == RegistrationStatus.registered)
        & (
            ratings_df[CQCRatings.current_or_historic]
            == CQCCurrentOrHistoricValues.current
        )
    )
    return benchmark_ratings_df


def add_good_and_outstanding_flag_column(benchmark_ratings_df: DataFrame) -> DataFrame:
    benchmark_ratings_df = benchmark_ratings_df.withColumn(
        CQCRatings.good_or_outstanding_flag,
        F.when(
            (benchmark_ratings_df[CQCRatings.overall_rating] == CQCRatingsValues.good)
            | (
                benchmark_ratings_df[CQCRatings.overall_rating]
                == CQCRatingsValues.outstanding
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    return benchmark_ratings_df


def join_establishment_ids(
    benchmark_ratings_df: DataFrame, ascwds_workplace_df: DataFrame
) -> DataFrame:
    ascwds_workplace_df = ascwds_workplace_df.select(
        ascwds_workplace_df[AWP.location_id].alias(CQCL.location_id),
        ascwds_workplace_df[AWP.establishment_id],
    )
    benchmark_ratings_df = benchmark_ratings_df.join(
        ascwds_workplace_df, CQCL.location_id, "left"
    )
    return benchmark_ratings_df


def create_benchmark_ratings_dataset(benchmark_ratings_df: DataFrame) -> DataFrame:
    benchmark_ratings_df = benchmark_ratings_df.select(
        benchmark_ratings_df[CQCL.location_id].alias(CQCRatings.benchmarks_location_id),
        benchmark_ratings_df[AWP.establishment_id].alias(
            CQCRatings.benchmarks_establishment_id
        ),
        benchmark_ratings_df[CQCRatings.good_or_outstanding_flag],
        benchmark_ratings_df[CQCRatings.overall_rating].alias(
            CQCRatings.benchmarks_overall_rating
        ),
        benchmark_ratings_df[CQCRatings.date].alias(CQCRatings.inspection_date),
    )
    benchmark_ratings_df = benchmark_ratings_df.where(
        benchmark_ratings_df[CQCRatings.benchmarks_establishment_id].isNotNull()
        & benchmark_ratings_df[CQCRatings.overall_rating].isNotNull()
    )
    return benchmark_ratings_df


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
