import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max
from pyspark.sql.types import IntegerType
from utils import utils


def clean(input_df):
    # Standardise negative and 0 values as None.
    input_df = input_df.replace('0', None).replace('-1', None)

    # Cast integers to string
    input_df = input_df.withColumn(
        "totalstaff", input_df["totalstaff"].cast(IntegerType()))

    input_df = input_df.withColumn(
        "wkrrecs", input_df["wkrrecs"].cast(IntegerType()))

    return input_df


def filter_nulls(input_df):
    # Remove rows with null for wkrrecs and totalstaff
    input_df = input_df.filter("wkrrecs is not null or totalstaff is not null")

    # Remove rows with null locationId
    input_df = input_df.na.drop(subset=["locationid"])

    return input_df


def calculate_jobcount(input_df):
    # Add null jobcount column
    input_df = input_df.withColumn("jobcount", lit(None).cast(IntegerType()))

    # totalstaff = wkrrrecs: Take totalstaff
    input_df = (
        input_df
        .filter("jobcount is null")
        .filter("wkrrecs=totalstaff and wkrrecs is not null and totalstaff is not null")
        .withColumn("jobcount", input_df.totalstaff)
    )

    # Either wkrrecs or totalstaff is null: return first not null
    input_df = (
        input_df
        .filter("jobcount is null")
        .filter("wkrrecs is null or totalstaff is null")
        .withColumn("jobcount", coalesce(input_df.totalstaff, input_df.wkrrecs))
    )

    # Abs difference between totalstaff & wkrrecs < 5 or < 10% take average:
    input_df = input_df.withColumn('abs_difference', abs(
        input_df.totalstaff - input_df.wkrrecs))

    input_df = (
        input_df
        .filter("jobcount is null")
        .filter("abs_difference < 8 or abs_difference/totalstaff < 0.15")
        .withColumn("jobcount", (input_df.totalstaff + input_df.wkrrecs)/2)
    )

    input_df = input_df.drop("abs_difference")

    # totalstaff or wkrrecs < 3: return max
    input_df = (
        input_df
        .filter("jobcount is null")
        .filter("totalstaff < 3 or wkrrecs < 3")
        .withColumn("jobcount", greatest(input_df.totalstaff, input_df.wkrrecs))
    )

    # Estimate job count from beds

    bed_estimate_df = input_df.filter(
        "numberofbeds is not null and numberofbeds > 0")

    input_df = (
        input_df
        .filter("jobcount is null")
        .filter("numberofbeds > 0")
        .withColumn("bed_estimate_jobcount", 8.40975704621392 + bed_estimate_df.numberofbeds * 1.0010753137758377001)
    )
