import csv

import boto3
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import SpecialistGeneralistOther
from utils.utils import split_s3_uri


def classify_specialisms(
    df: DataFrame,
    specialism: str,
) -> DataFrame:
    """
    Classifies the specialisms offered and creates a new column for that specialism to indicate if it's specialist/generalist/other
    Specialist - when the named specialism is the only one offered by the location.
    Generalist - when the named specialism is offered alongside others.
    Other - the named specialism is not offered.

    Args:
        df (DataFrame): CQC registered locations DataFrame with the specialisms offered column.
        specialism (str): Unique specialism name we want to create new column for.

    Returns:
        DataFrame: Updated dataFrame with new column for specified specialism

    """
    new_column_name: str = f"specialist_generalist_other_{specialism}"
    new_column_name = new_column_name.replace(" ", "_").lower()
    df = df.withColumn(
        new_column_name,
        F.when(
            F.array_contains(F.col(IndCQC.specialisms_offered), specialism)
            & (F.size(F.col(IndCQC.specialisms_offered)) == 1),
            SpecialistGeneralistOther.specialist,
        )
        .when(
            F.array_contains(F.col(IndCQC.specialisms_offered), specialism)
            & (F.size(F.col(IndCQC.specialisms_offered)) > 1),
            SpecialistGeneralistOther.generalist,
        )
        .otherwise(SpecialistGeneralistOther.other),
    )

    return df


def read_manual_postcode_corrections_csv_to_dict(
    source: str, s3_client: object = None
) -> dict:
    """
    Read csv of postcode corrections from given location to a dictionary.

    Args:
        source(str): The s3 URI of the incorrect postcode csv file
        s3_client(object): An s3 client

    Returns:
        dict: A dictionary of postcode corrections in the format {incorrect: correct}
    """
    bucket, key = split_s3_uri(source)
    if s3_client is None:
        s3_client = boto3.client("s3")
    postcode_obj = s3_client.get_object(Bucket=bucket, Key=key)

    postcode_data = postcode_obj["Body"].read().decode("utf-8").splitlines()
    postcode_records = csv.reader(postcode_data)
    headers = next(postcode_records)
    postcode_dict = {record[0]: record[1] for record in postcode_records}
    return postcode_dict
