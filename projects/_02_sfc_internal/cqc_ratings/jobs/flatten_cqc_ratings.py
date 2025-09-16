import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from schemas.cqc_location_schema import LOCATION_SCHEMA
from utils import cleaning_utils as cUtils
from utils import utils
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCRatingsValues,
    LocationType,
    RegistrationStatus,
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
    CQCL.assessment,
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
    cqc_location_df = utils.read_from_parquet(
        cqc_location_source, cqc_location_columns, LOCATION_SCHEMA
    )
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
    assessment_ratings_df = prepare_assessment_ratings(cqc_location_df)

    raise_error_when_assessment_df_contains_overall_data(assessment_ratings_df)

    ratings_pre_saf_df = current_ratings_df.unionByName(historic_ratings_df)
    ratings_df = merge_cqc_ratings(assessment_ratings_df, ratings_pre_saf_df)

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
    ratings_df = recode_unknown_codes_to_null(
        ratings_df
    )  # creates duplicates as differenct codes are reduced to the same (null) value
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


def prepare_assessment_ratings(cqc_location_df: DataFrame) -> DataFrame:
    """
    Flatten overall and ASG ratings within assessment field extracted from CQC location data into a unified, pivoted DataFrame.

    This function:
      1. Extracts assessment base information from the input location DataFrame.
      2. Separately flattens overall ratings and ASG ratings into tabular form.
      3. Unions both datasets together, aligning by schema.
      4. Pivots key question ratings into columns (Safe, Effective, Caring, Responsive and Well-led.).

    Args:
        cqc_location_df (DataFrame): Input DataFrame containing raw CQC location data, with nested assessments and ratings.

    Returns:
        DataFrame: Flattened DataFrame where each row corresponds to a locations assessment plan, with key question ratings pivoted into individual columns.
    """
    assessment_df = extract_assessment_base(cqc_location_df)
    overall_df = extract_overall(assessment_df)
    asg_df = extract_asg(assessment_df)

    union_df = overall_df.unionByName(asg_df, allowMissingColumns=True)

    final_df = (
        union_df.groupBy(
            CQCL.location_id,
            CQCL.registration_status,
            CQCL.assessment_plan_published_datetime,
            CQCL.assessment_plan_id,
            CQCL.title,
            CQCL.assessment_date,
            CQCL.assessment_plan_status,
            CQCL.dataset,
            CQCL.name,
            CQCL.status,
            CQCL.rating,
            CQCL.source_path,
        )
        .pivot(CQCL.key_question_name)
        .agg(F.first(CQCL.key_question_rating))
        .orderBy(CQCL.assessment_date)
    )

    desired_column_order = [
        CQCL.location_id,
        CQCL.registration_status,
        CQCL.assessment_plan_published_datetime,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.dataset,
        CQCL.name,
        CQCL.status,
        CQCL.rating,
        CQCL.source_path,
        CQCL.safe,
        CQCL.effective,
        CQCL.caring,
        CQCL.responsive,
        CQCL.well_led,
    ]

    return final_df.select(*desired_column_order)


def extract_assessment_base(cqc_location_df: DataFrame) -> DataFrame:
    """
    Extract and explode the base assessment data from the CQC location dataset.

    Args:
        cqc_location_df (DataFrame): Input DataFrame containing raw CQC location data with nested assessments.

    Returns:
        DataFrame: DataFrame with columns for location ID, registration status, assessment plan published datetime, and nested assessment ratings.
    """
    assessment_base_df = cqc_location_df.withColumn(
        CQCL.assessment_exploded, F.explode(CQCL.assessment)
    ).select(
        CQCL.location_id,
        CQCL.registration_status,
        F.col(
            f"{CQCL.assessment_exploded}.{CQCL.assessment_plan_published_datetime}"
        ).alias(CQCL.assessment_plan_published_datetime),
        F.col(f"{CQCL.assessment_exploded}.{CQCL.ratings}").alias(
            CQCL.assessments_ratings
        ),
    )
    return assessment_base_df


def extract_overall(assessment_df: DataFrame) -> DataFrame:
    """
    Extract and flatten 'overall' ratings from assessment data.

    This function:
      1. Explodes the `overall` ratings array.
      2. Selects Explodes core fields such as rating, status, and key question ratings.

    Args:
        assessment_df (DataFrame): DataFrame produced by `extract_assessment_base`, containing assessment ratings.

    Returns:
        DataFrame: Flattened DataFrame of overall ratings, including key question ratings for each assessment.
    """
    exploded = assessment_df.withColumn(
        CQCL.overall_exploded,
        F.explode(F.col(f"{CQCL.assessments_ratings}.{CQCL.overall}")),
    )

    flattened_df = exploded.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCL.assessment_plan_published_datetime,
        F.col(f"{CQCL.overall_exploded}.{CQCL.rating}").alias(CQCL.rating),
        F.col(f"{CQCL.overall_exploded}.{CQCL.status}").alias(CQCL.status),
        F.col(f"{CQCL.overall_exploded}.{CQCL.key_question_ratings}").alias(
            CQCL.key_question_ratings
        ),
    )

    overall_df = flattened_df.withColumn(
        CQCL.overall_key_questions_exploded, F.explode(CQCL.key_question_ratings)
    ).select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCL.assessment_plan_published_datetime,
        CQCL.rating,
        CQCL.status,
        F.col(f"{CQCL.overall_key_questions_exploded}.{CQCL.name}").alias(
            CQCL.key_question_name
        ),
        F.col(f"{CQCL.overall_key_questions_exploded}.{CQCL.rating}").alias(
            CQCL.key_question_rating
        ),
        F.col(f"{CQCL.overall_key_questions_exploded}.{CQCL.status}").alias(
            CQCL.key_question_status
        ),
        F.lit("SAF").alias(CQCL.dataset),
        F.lit("assessment.ratings.overall").alias(CQCL.source_path),
    )
    return overall_df


def raise_error_when_assessment_df_contains_overall_data(df: DataFrame) -> None:
    """
    Raise an error when the assessments dataframe contains any overall ratings data.

    Currently, CQC publish an overall rating object within the assessments column, but this is not populated
    for any social care locations we've checked as at 15/09/2025. It is published for non-social care locations.
    This overall rating object can have it's own "rating" and "key question ratings".

    CQC also publish a "rating" and "key question ratings" within the asg_ratings object within the assessments column.
    This "rating" and "key question ratings" are at the service level within a location (one location can have many services).
    We are refering to this "rating" as the overall rating for social care locations.

    This function raises a value error if the overall object contains any values.
    If this happens, we need to refactor the flattening of CQC assessments data.

    Args:
        df (DataFrame): Dataframe of flattened CQC assessments data.

    Raises:
        ValueError: If the DataFrame contains overall assessements data.
    """
    rows_where_overall_has_value = df.where(
        (F.col(CQCL.source_path) == "assessment.ratings.overall")
        & (F.col(CQCL.rating).isNotNull())
    ).count()

    if rows_where_overall_has_value > 0:
        raise ValueError(
            f"The overall object within the assessments column contains {rows_where_overall_has_value} values for social care locations."
        )

    return None


def extract_asg(assessment_df: DataFrame) -> DataFrame:
    """
    Extract and flatten ASG ratings from assessment data.

    This function:
      1. Explodes the `asg_ratings` array.
      2. Selects core ASG fields such as plan ID, title, assessment date, status, and rating.
      3. Explodes the nested key question ratings so each becomes its own row.
      4. Adds additional fields including percentage score (if present), dataset type, and source path.

    Args:
        assessment_df (DataFrame): DataFrame produced by `extract_assessment_base`, containing assessment ratings.

    Returns:
        DataFrame: Flattened DataFrame of ASG ratings, including key question ratings and metadata for each sub assessment.
    """
    exploded = assessment_df.withColumn(
        CQCL.asg_exploded,
        F.explode(F.col(f"{CQCL.assessments_ratings}.{CQCL.asg_ratings}")),
    )

    flattened = exploded.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCL.assessment_plan_published_datetime,
        F.col(f"{CQCL.asg_exploded}.{CQCL.assessment_plan_id}").alias(
            CQCL.assessment_plan_id
        ),
        F.col(f"{CQCL.asg_exploded}.{CQCL.title}").alias(CQCL.title),
        F.col(f"{CQCL.asg_exploded}.{CQCL.assessment_date}").alias(
            CQCL.assessment_date
        ),
        F.col(f"{CQCL.asg_exploded}.{CQCL.assessment_plan_status}").alias(
            CQCL.assessment_plan_status
        ),
        F.col(f"{CQCL.asg_exploded}.{CQCL.name}").alias(CQCL.name),
        F.col(f"{CQCL.asg_exploded}.{CQCL.rating}").alias(CQCL.rating),
        F.col(f"{CQCL.asg_exploded}.{CQCL.status}").alias(CQCL.status),
        F.col(f"{CQCL.asg_exploded}.{CQCL.key_question_ratings}").alias(
            CQCL.key_question_ratings
        ),
    )

    asg_df = flattened.withColumn(
        CQCL.asg_key_questions_exploded, F.explode(CQCL.key_question_ratings)
    ).select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCL.assessment_plan_published_datetime,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.rating,
        CQCL.status,
        F.col(f"{CQCL.asg_key_questions_exploded}.{CQCL.name}").alias(
            CQCL.key_question_name
        ),
        F.col(f"{CQCL.asg_key_questions_exploded}.{CQCL.rating}").alias(
            CQCL.key_question_rating
        ),
        F.col(f"{CQCL.asg_key_questions_exploded}.{CQCL.status}").alias(
            CQCL.key_question_status
        ),
        F.col(f"{CQCL.asg_key_questions_exploded}.{CQCL.percentage_score}").alias(
            CQCL.key_question_percentage_score
        ),
        F.lit("SAF").alias(CQCL.dataset),
        F.lit("assessment.ratings.asg_ratings").alias(CQCL.source_path),
    )
    return asg_df


def merge_cqc_ratings(
    assessment_ratings_df: DataFrame,
    standard_ratings_df: DataFrame,
) -> DataFrame:
    """
    Function to merge assessment_ratings_df and standard_ratings_df to get final ratings

    Args:
        assessment_ratings_df (DataFrame): DataFrame produced by `prepare_assessment_ratings`, containing flattened assessment ratings.
        standard_ratings_df (DataFrame): DataFrame produced by flattening standard cqc ratings df.

    Returns:
        DataFrame: Merged DataFrame of Old CQC ratings and the new assessment ASG ratings, including key question ratings and metadata for each sub assessment.
    """

    expected_columns = [
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        CQCRatings.current_or_historic,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
    ]
    standard_df = standard_ratings_df.select(
        CQCL.location_id,
        CQCL.registration_status,
        CQCRatings.date,
        CQCRatings.current_or_historic,
        CQCRatings.overall_rating,
        CQCRatings.safe_rating,
        CQCRatings.well_led_rating,
        CQCRatings.caring_rating,
        CQCRatings.responsive_rating,
        CQCRatings.effective_rating,
        F.lit("Pre SAF").alias(CQCL.dataset),
    )
    assessment_df = assessment_ratings_df.select(
        CQCL.location_id,
        CQCL.registration_status,
        F.to_date(
            F.to_timestamp(
                CQCL.assessment_plan_published_datetime, "yyyy-MM-dd HH:mm:ss"
            )
        ).alias(CQCRatings.date),
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        F.col(CQCL.status).alias(CQCRatings.current_or_historic),
        F.col(CQCL.rating).alias(CQCRatings.overall_rating),
        F.col(CQCL.safe).alias(CQCRatings.safe_rating),
        F.col(CQCL.well_led).alias(CQCRatings.well_led_rating),
        F.col(CQCL.caring).alias(CQCRatings.caring_rating),
        F.col(CQCL.responsive).alias(CQCRatings.responsive_rating),
        F.col(CQCL.effective).alias(CQCRatings.effective_rating),
    )
    merged_df = standard_df.unionByName(assessment_df, allowMissingColumns=True)
    return merged_df.select(*expected_columns)


def recode_unknown_codes_to_null(ratings_df: DataFrame) -> DataFrame:
    ratings_df = cUtils.apply_categorical_labels(
        ratings_df,
        UnknownRatings,
        UnknownRatings.keys(),
        add_as_new_column=False,
    )
    ratings_df = ratings_df.drop_duplicates()
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
        CQCRatings.overall_rating: CQCRatings.overall_rating_value,
        CQCRatings.safe_rating: CQCRatings.safe_rating_value,
        CQCRatings.well_led_rating: CQCRatings.well_led_rating_value,
        CQCRatings.caring_rating: CQCRatings.caring_rating_value,
        CQCRatings.responsive_rating: CQCRatings.responsive_rating_value,
        CQCRatings.effective_rating: CQCRatings.effective_rating_value,
    }
    for rating_column, new_column_name in rating_columns_dict.items():
        df = df.withColumn(
            new_column_name,
            F.when(
                F.lower(F.col(rating_column)) == CQCRatingsValues.outstanding.lower(),
                F.lit(4),
            )
            .when(
                F.lower(F.col(rating_column)) == CQCRatingsValues.good.lower(), F.lit(3)
            )
            .when(
                F.lower(F.col(rating_column))
                == CQCRatingsValues.requires_improvement.lower(),
                F.lit(2),
            )
            .when(
                F.lower(F.col(rating_column)) == CQCRatingsValues.inadequate.lower(),
                F.lit(1),
            )
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
        CQCL.registration_status,
        CQCRatings.date,
        CQCL.assessment_plan_id,
        CQCL.title,
        CQCL.assessment_date,
        CQCL.assessment_plan_status,
        CQCL.name,
        CQCL.source_path,
        CQCL.dataset,
        CQCRatings.current_or_historic,
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
    w = Window.partitionBy(CQCL.location_id)

    benchmark_ratings_df = benchmark_ratings_df.withColumn(
        CQCRatings.good_or_outstanding_flag,
        F.when(
            F.min(CQCRatings.overall_rating_value).over(w) >= 3,
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
