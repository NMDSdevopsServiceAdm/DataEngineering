import argparse
from datetime import date

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import abs, coalesce, greatest, lit, max, when, col
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
    # Add null/empty jobcount column
    input_df = input_df.withColumn("jobcount", lit(None).cast(IntegerType()))

    # totalstaff = wkrrrecs: Take totalstaff
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (col("wkrrecs") == col("totalstaff")) &
            col("totalstaff").isNotNull() &
            col("wkrrecs").isNotNull()
        ), col("totalstaff")
    ).otherwise(col("jobcount")))

    # Either wkrrecs or totalstaff is null: return first not null
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (
                    col("totalstaff").isNull() &
                    col("wkrrecs").isNotNull()
                ) |
                (
                    col("totalstaff").isNotNull() &
                    col("wkrrecs").isNull()
                )
            )
        ), coalesce(input_df.totalstaff, input_df.wkrrecs)
    ).otherwise(coalesce(col("jobcount"))))

    # Abs difference between totalstaff & wkrrecs < 5 or < 10% take average:
    input_df = input_df.withColumn('abs_difference', abs(
        input_df.totalstaff - input_df.wkrrecs))

    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (col("abs_difference") < 5) | (
                    col("abs_difference") / col("totalstaff") < 0.1)
            )
        ), (col("totalstaff") + col("wkrrecs")) / 2
    ).otherwise(col("jobcount")))

    input_df = input_df.drop("abs_difference")

    # totalstaff or wkrrecs < 3: return max
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            (
                (col("totalstaff") < 3) | (col("wkrrecs") < 3)
            )
        ), greatest(col("totalstaff"), col("wkrrecs"))
    ).otherwise(col("jobcount")))

    # Estimate job count from beds
    input_df = input_df.withColumn("bed_estimate_jobcount", when(
        (
            col("jobcount").isNull() &
            (col("numberofbeds") > 0)
        ), (8.40975704621392 + (col("numberofbeds") * 1.0010753137758377001))
    ).otherwise(None))

    # Determine differences
    input_df = input_df.withColumn("totalstaff_diff", abs(
        input_df.totalstaff - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_diff", abs(
        input_df.wkrrecs - input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("totalstaff_percentage_diff", abs(
        input_df.totalstaff_diff/input_df.bed_estimate_jobcount))
    input_df = input_df.withColumn("wkrrecs_percentage_diff", abs(
        input_df.wkrrecs/input_df.bed_estimate_jobcount))

    # Bounding predictions to certain locations with differences in range
    # if totalstaff and wkrrecs within 10% or < 5: return avg(totalstaff + wkrrds)
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (
                (
                    (col("totalstaff_diff") < 5) | (
                        col("totalstaff_percentage_diff") < 0.1)
                ) &
                (
                    (col("wkrrecs_diff") < 5) | (
                        col("wkrrecs_percentage_diff") < 0.1)
                )
            )
        ), (col("totalstaff") + col("wkrrecs")) / 2
    ).otherwise(col("jobcount")))

    # if totalstaff within 10% or < 5: return totalstaff
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (

                (col("totalstaff_diff") < 5) | (
                    col("totalstaff_percentage_diff") < 0.1)

            )
        ), col("totalstaff")
    ).otherwise(col("jobcount")))

    # if wkrrecs within 10% or < 5: return wkrrecs
    input_df = input_df.withColumn("jobcount", when(
        (
            col("jobcount").isNull() &
            col("bed_estimate_jobcount").isNotNull() &
            (

                (col("wkrrecs_diff") < 5) | (
                    col("wkrrecs_percentage_diff") < 0.1)

            )
        ), col("wkrrecs")
    ).otherwise(col("jobcount")))

    return input_df
