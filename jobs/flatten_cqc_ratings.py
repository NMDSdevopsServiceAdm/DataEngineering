import sys
from pyspark.sql import DataFrame, functions as F

from utils import utils

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


cqc_location_columns = [
    CQCL.location_id,
    Keys.import_date,
    CQCL.current_ratings,
    CQCL.historic_ratings,
    CQCL.registration_status,
    CQCL.type,
]

ascwds_workplace_columns =[
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
    ascwds_workplace_df = utils.read_from_parquet(ascwds_workplace_source, ascwds_workplace_columns)

    cqc_location_df = filter_to_monthly_import_date(cqc_location_df)

    cqc_location_df = utils.select_rows_with_value(cqc_location_df, CQCL.type, CQCLValues.social_care_identifier)

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
        cqc_location_df,
        cqc_ratings_destination,
        mode="overwrite",
    )

    # select ratings for benchmarks
    # create flag for good and outsatnding
    # add establishment ids
    # select rows and columns to save

    # save ratings for benchmarks


def filter_to_monthly_import_date(cqc_location_df: DataFrame) -> DataFrame:
    max_import_date = cqc_location_df.agg(F.max(cqc_location_df[Keys.import_date])).collect()[0][0]
    first_day_of_the_month = "01"
    month_and_year_of_import_date = max_import_date[0:6]
    monthly_import_date = month_and_year_of_import_date + first_day_of_the_month
    cqc_location_df = cqc_location_df.where(cqc_location_df[Keys.import_date] == monthly_import_date)
    return cqc_location_df

def prepare_current_ratings(cqc_location_df: DataFrame) -> DataFrame:
    ratings_df = flatten_current_ratings(cqc_location_df)
    ratings_df = recode_unknown_codes_to_null(ratings_df)
    ratings_df = add_current_or_historic_column(ratings_df, "current")
    return cqc_location_df

def flatten_current_ratings(cqc_location_df:DataFrame) -> DataFrame:
    return cqc_location_df

def recode_unknown_codes_to_null(ratings_df:DataFrame) -> DataFrame:
    return ratings_df

def add_current_or_historic_column(ratings_df:DataFrame, current_or_historic:str) -> DataFrame:
    current_or_historic = "current"
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
