from utils import utils
from pyspark.sql.functions import col
from datetime import date
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, FloatType, IntegerType


def generate_ethnicity_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "mainjrid", "ethnicity", "import_date"]

    rows = [
        ("1-000000001", "1", "31", "20200301"),
        ("1-000000001", "1", "32", "20200301"),
        ("1-000000001", "7", "33", "20200301"),
        ("1-000000001", "8", "35", "20200301"),
        ("1-000000001", "8", "39", "20200301"),
        ("1-000000001", "8", "99", "20200301"),
        ("1-000000001", "8", "-1", "20200301"),
        ("1-000000002", "1", "34", "20200301"),
        ("1-000000002", "8", "31", "20200301"),
        ("1-000000002", "7", "32", "20200301"),
        ("1-000000003", "1", "33", "20200301"),
        ("1-000000003", "8", "34", "20200301"),
        ("1-000000003", "8", "98", "20200301"),
        ("1-000000004", "1", "39", "20200301"),
        ("1-000000004", "8", "46", "20200301"),
        ("1-000000004", "8", "37", "20200301"),
        ("1-000000004", "1", "31", "20190301"),
        ("1-000000004", "1", "31", "20210301"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_all_job_roles_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["master_locationid", "primary_service_type", "main_job_role", "estimate_job_role_count_2021"]

    rows = [
        ("1-000000001", "Care home without nursing", "1", 0.5),
        ("1-000000001", "Care home without nursing", "7", 3.5),
        ("1-000000001", "Care home without nursing", "8", 10.0),
        ("1-000000002", "Care home without nursing", "1", 2.0),
        ("1-000000002", "Care home without nursing", "7", 8.0),
        ("1-000000002", "Care home without nursing", "8", 20.0),
        ("1-000000003", "Care home with nursing", "1", 0.0),
        ("1-000000003", "Care home with nursing", "7", 1.0),
        ("1-000000003", "Care home with nursing", "8", 5.0),
        ("1-000000004", "Care home with nursing", "1", 10.0),
        ("1-000000004", "Care home with nursing", "7", 0.0),
        ("1-000000004", "Care home with nursing", "8", 10.0),
        ("1-000000005", "Care home with nursing", "1", 11.0),
        ("1-000000005", "Care home with nursing", "7", 22.0),
        ("1-000000005", "Care home with nursing", "8", 33.0),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_cqc_locations_prepared_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "providerid", "postal_code", "version"]

    rows = [
        ("1-000000001", "1-000000001", "AB1 2CD", "1.0.3"),
        ("1-000000002", "1-000000001", "WX9 0YZ", "1.0.3"),
        ("1-000000003", "1-000000001", "WX9 0YZ", "1.0.3"),
        ("1-000000004", "1-000000001", "AB1 2CD", "1.0.3"),
        ("1-000000005", "1-000000001", "WX9 0YZ", "1.0.3"),
        ("1-000000006", "1-000000001", "AB1 2CD", "1.0.0"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ons_geography_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["pcds", "lsoa11", "msoa11", "rgn", "ctry", "other_cols"]

    rows = [
        ("AB1 2CD", "E01000001", "E02000003", "E12000001", "E92000001", "other geography stuff"),
        ("WX9 0YZ", "E01000003", "E02000002", "E12000001", "E92000001", "other geography stuff"),
        ("GH5 6IJ", "E01000002", "E02000001", "E12000002", "E92000001", "other geography stuff"),
        ("ZZ2 2ZZ", "S01000002", "S02000001", "S12000002", "S92000001", "other geography stuff"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ethnicity_census_lsoa_csv(output_destination):
    spark = utils.get_spark()
    columns = [
        "2011 super output area - lower layer",
        "All categories: Ethnic group of HRP",
        "White: Total",
        "White: English/Welsh/Scottish/Northern Irish/British",
        "White: Irish",
        "White: Other White",
        "Mixed/multiple ethnic group",
        "Asian/Asian British",
        "Black/African/Caribbean/Black British",
        "Other ethnic group",
    ]

    rows = [
        ("E01000001 : Area name 001A", "876", "767", "608", "18", "141", "15", "74", "4", "16"),
        ("E01000002 : Area name 001B", "830", "763", "630", "13", "120", "16", "45", "2", "4"),
        ("E01000003 : Area name 001C", "817", "678", "533", "26", "119", "22", "84", "20", "13"),
        ("E01000005 : Area name 001E", "467", "311", "222", "11", "78", "23", "77", "37", "19"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_destination)

    return df


def generate_estimate_jobs_2021_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "primary_service_type", "estimate_job_count_2021"]

    rows = [
        ("1-000000001", "Care home without nursing", 15.5),
        ("1-000000002", "Care home with nursing", 90.0),
        ("1-000000003", "Care home with nursing", 2.1),
        ("1-000000004", "non-residential", 25.3),
        ("1-000000005", "non-residential", 94.0),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_worker_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "workerid", "mainjrid"]

    rows = [
        ("1-000000001", "100", 1),
        ("1-000000001", "101", 1),
        ("1-000000001", "102", 1),
        ("1-000000001", "103", 1),
        ("1-000000001", "104", 2),
        ("1-000000001", "105", 3),
        ("1-000000002", "106", 1),
        ("1-000000002", "107", 3),
        ("1-000000002", "108", 2),
        ("1-000000003", "109", 1),
        ("1-000000003", "110", 2),
        ("1-000000003", "111", 3),
        ("1-000000004", "112", 1),
        ("1-000000004", "113", 2),
        ("1-000000004", "114", 3),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ascwds_workplace_file(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "establishmentid", "totalstaff", "wkrrecs", "import_date", "orgid", "mupddate", "isparent"]

    rows = [
        ("1-000000001", "101", 14, 16, "20220101", "1", date(2021, 2, 1), 0),
        ("1-000000002", "102", 76, 65, "20220101", "1", date(2021, 4, 1), 1),
        ("1-000000003", "103", 34, 34, "20220101", "2", date(2021, 3, 1), 0),
        ("1-000000004", "104", 234, 265, "20220101", "2", date(2021, 4, 1), 0),
        ("1-000000005", "105", 62, 65, "20220101", "3", date(2021, 10, 1), 0),
        ("1-000000006", "106", 77, 77, "20220101", "3", date(2020, 3, 1), 1),
        ("1-000000007", "107", 51, 42, "20220101", "3", date(2021, 5, 1), 0),
        ("1-000000008", "108", 36, 34, "20220101", "4", date(2021, 7, 1), 0),
        ("1-000000009", "109", 34, 32, "20220101", "5", date(2021, 12, 1), 0),
        ("1-0000000010", "110", 14, 20, "20220101", "6", date(2021, 3, 1), 0),
        ("1-000000001", "101", 14, 16, "20210101", "1", date(2021, 2, 1), 0),
        ("1-000000002", "102", 76, 65, "20210101", "1", date(2021, 4, 1), 1),
        ("1-000000003", "103", 34, 34, "20210101", "2", date(2021, 3, 1), 0),
        ("1-000000004", "104", 234, 265, "20210101", "2", date(2021, 4, 1), 0),
        ("1-000000005", "105", 62, 65, "20210101", "3", date(2021, 10, 1), 0),
        ("1-000000006", "106", 77, 77, "20210101", "3", date(2020, 3, 1), 1),
        ("1-000000007", "107", 51, 42, "20210101", "3", date(2021, 5, 1), 0),
        ("1-000000008", "108", 36, 34, "20210101", "4", date(2021, 7, 1), 0),
        ("1-000000009", "109", 34, 32, "20210101", "5", date(2021, 12, 1), 0),
        ("1-0000000010", "110", 14, 20, "20210101", "6", date(2021, 3, 1), 0),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df
