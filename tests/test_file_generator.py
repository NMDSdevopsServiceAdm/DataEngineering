from datetime import date
import dbldatagen as dg

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    IntegerType,
)

from schemas import cqc_care_directory_schema, worker_schema
from utils import utils


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


def generate_all_job_roles_parquet(output_destination):
    spark = utils.get_spark()
    columns = [
        "master_locationid",
        "primary_service_type",
        "main_job_role",
        "estimate_job_role_count_2021",
    ]

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
        (
            "AB1 2CD",
            "E01000001",
            "E02000003",
            "E12000001",
            "E92000001",
            "other geography stuff",
        ),
        (
            "WX9 0YZ",
            "E01000003",
            "E02000002",
            "E12000001",
            "E92000001",
            "other geography stuff",
        ),
        (
            "GH5 6IJ",
            "E01000002",
            "E02000001",
            "E12000002",
            "E92000001",
            "other geography stuff",
        ),
        (
            "ZZ2 2ZZ",
            "S01000002",
            "S02000001",
            "S12000002",
            "S92000001",
            "other geography stuff",
        ),
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
        (
            "E01000001 : Area name 001A",
            "876",
            "767",
            "608",
            "18",
            "141",
            "15",
            "74",
            "4",
            "16",
        ),
        (
            "E01000002 : Area name 001B",
            "830",
            "763",
            "630",
            "13",
            "120",
            "16",
            "45",
            "2",
            "4",
        ),
        (
            "E01000003 : Area name 001C",
            "817",
            "678",
            "533",
            "26",
            "119",
            "22",
            "84",
            "20",
            "13",
        ),
        (
            "E01000005 : Area name 001E",
            "467",
            "311",
            "222",
            "11",
            "78",
            "23",
            "77",
            "37",
            "19",
        ),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            output_destination
        )

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


def generate_cqc_locations_file(output_destination):
    spark = utils.get_spark()

    schema = StructType(
        fields=[
            StructField("locationId", StringType(), True),
            StructField("providerId", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("registrationStatus", StringType(), True),
            StructField("registrationDate", StringType(), True),
            StructField("deregistrationDate", StringType(), True),
            StructField("dormancy", StringType(), True),
            StructField("numberOfBeds", IntegerType(), True),
            StructField("region", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("careHome", StringType(), True),
            StructField("constituency", StringType(), True),
            StructField("localAuthority", StringType(), True),
            StructField(
                "gacServiceTypes",
                ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("description", StringType(), True),
                        ]
                    )
                ),
            ),
            StructField("import_date", StringType(), True),
        ]
    )

    # fmt: off
    rows = [
        ("1-000000001", 1, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire & Humberside", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000002", 2, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000003", 3, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000004", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "Yorkshire & Humberside", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000005", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000006", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000007", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000008", 6, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000009", 7, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20220101"),
        ("1-000000010", 8, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000011", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000012", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000013", 10, "location", "Social Care Org", "name of organisation", "Deregistered", "2011-02-15", "2015-01-01", "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000014", 11, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000015", 12, "location", "Not social care", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000001", 1, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000002", 2, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000003", 3, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000004", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000005", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000006", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000007", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000008", 6, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000009", 7, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000010", 8, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000011", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000012", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000013", 10, "location", "Social Care Org", "name of organisation", "Deregistered", "2011-02-15", "2015-01-01", "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000014", 11, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000015", 12, "location", "Not social care", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, schema)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_cqc_providers_file(output_destination):
    spark = utils.get_spark()

    schema = StructType(
        fields=[
            StructField("providerId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("import_date", StringType(), True),
        ]
    )

    rows = [
        (1, "new provider name 1", "20220105"),
        (2, "provider name 2", "20220105"),
        (3, "provider name 3", "20220105"),
        (1, "provider name 1", "20210105"),
        (2, "provider name 2", "20210105"),
        (3, "provider name 3", "20210105"),
        (1, "provider name 1", "20200105"),
        (2, "provider name 2", "20200105"),
        (3, "provider name 3", "20200105"),
    ]

    df = spark.createDataFrame(rows, schema)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_pir_file(output_destination):
    spark = utils.get_spark()

    schema = StructType(
        fields=[
            StructField("location_id", StringType(), True),
            StructField(
                "21_How_many_people_are_currently_receiving_support"
                "_with_regulated_activities_as_defined_by_the_Health"
                "_and_Social_Care_Act_from_your_service",
                StringType(),
                True,
            ),
            StructField("import_date", StringType(), True),
        ]
    )

    rows = [
        ("1-000000001", 20, "20220105"),
        ("1-000000002", 10, "20220105"),
        ("1-000000003", 16, "20220105"),
        ("1-000000004", 29, "20220105"),
        ("1-000000005", 93, "20220105"),
        ("1-000000006", 32, "20220105"),
        ("1-000000007", 23, "20220105"),
        ("1-000000008", 42, "20220105"),
        ("1-000000009", 48, "20210105"),
        ("1-000000001", 20, "20210105"),
        ("1-000000002", 10, "20210105"),
        ("1-000000003", 16, "20210105"),
        ("1-000000004", 29, "20210105"),
        ("1-000000005", 93, "20210105"),
        ("1-000000006", 32, "20210105"),
        ("1-000000007", 23, "20210105"),
        ("1-000000008", 42, "20210105"),
        ("1-000000009", 48, "20210105"),
    ]
    df = spark.createDataFrame(rows, schema)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ascwds_workplace_file(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationid",
        "establishmentid",
        "totalstaff",
        "wkrrecs",
        "import_date",
        "orgid",
        "mupddate",
        "isparent",
    ]

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


def generate_raw_cqc_care_directory_csv_file(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationid",
        "registrationdate",
        "name",
        "type",
        "providerid",
    ]

    rows = [
        (
            "1-10000000001",
            "01/12/2020",
            "Location 1",
            "Social Care Org",
            "1-10000000001",
        ),
        (
            "1-10000000002",
            "01/12/2020",
            "Location 2",
            "Primary Medical Services",
            "1-10000000002",
        ),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.option("header", True).mode("overwrite").csv(
            output_destination
        )

    return df


def generate_cqc_care_directory_file(output_destination):
    spark = utils.get_spark()
    # fmt: off
    rows = [
        ("1-10000000001", date(2020, 12, 1), "Y", "Location 1", "Social Care Org", "7123456789", "*", "", 1, "Region 6", "LA 9", "7 main road", "", "", "", "AB4 5CD", "BD001", "BRAND NAME 1", "1-10000000001", "Provider 1", "", "www.website1.com", "1 ave", "", "", "", "AB1 2CD", "Surname1, First1", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",),
        ("1-10000000002", date(2021, 11, 4), "N", "Location 2", "Social Care Org", "7123456789", "Surname1, First1", "www.website.com", 0, "Region 4", "LA 8", "13 main road", "", "", "", "AB7 7CD", "-", "-", "1-10000000002", "Provider 2", "7123456789", "", "2 ave", "", "", "", "AB3 4CD", "Surname2, First2", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "",),
        ("1-10000000003", date(2022, 10, 7), "Y", "Location 3", "Social Care Org", "7123456789", "Surname1, First1", "", 5, "Region 5", "LA 6", "6 main road", "", "", "", "AB6 3CD", "-", "-", "1-10000000002", "Provider 2", "7123456789", "", "2 ave", "", "", "", "AB3 4CD", "Surname2, First2", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "", "", "", "", "", "Y", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "",),
        ("1-10000000004", date(2023, 9, 10), "N", "Location 4", "Social Care Org", "7123456789", "Surname2, First2", "www.website.com", 0, "Region 1", "LA 5", "95 main road", "", "", "", "AB8 4CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "", "Y", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "",),
        ("1-10000000005", date(2024, 8, 13), "Y", "Location 5", "Social Care Org", "7123456789", "Surname3, First3", "", 10, "Region 4", "LA 4", "5 main road", "", "", "", "AB2 2CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "", "", "", "", "Y", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "",),
        ("1-10000000006", date(2025, 7, 16), "N", "Location 6", "Social Care Org", "7123456789", "Surname4, First4", "www.website.com", 0, "Region 1", "LA 2", "2 main road", "", "", "", "AB7 2CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "", "", "Y", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "",),
        ("1-10000000007", date(2026, 6, 19), "Y", "Location 7", "Social Care Org", "7123456789", "Surname5, First5", "", 50, "Region 4", "LA 1", "33 main road", "", "", "", "AB3 1CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "", "",),
        ("1-10000000008", date(2017, 5, 22), "N", "Location 8", "Social Care Org", "", "Surname6, First6", "www.website.com", 0, "Region 6", "LA 8", "90 main road", "", "", "", "AB3 5CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "", "", "", "", "", "", "",),
        ("1-10000000009", date(2018, 4, 25), "Y", "Location 9", "Social Care Org", "7123456789", "Surname7, First7", "", 100, "Region 4", "LA 10", "61 main road", "", "", "", "AB8 1CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "", "Y", "Y", "Y", "", "", "", "", "", "", "", "",),
        ("1-10000000010", date(2029, 3, 28), "N", "Location 10", "Social Care Org", "7123456789", "*", "www.website.com", 0, "Region 2", "LA 6", "56 main road", "", "", "", "AB4 4CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "Y", "Y", "", "", "", "", "", "", "", "", "", "",),
    ]
    # fmt: on

    df = spark.createDataFrame(
        rows, schema=cqc_care_directory_schema.CQC_CARE_DIRECTORY_SCHEMA
    )

    if output_destination:
        df.coalesce(1).write.option("header", True).mode("overwrite").csv(
            output_destination
        )

    return df


def generate_ascwds_worker_file(output_destination):
    spark = utils.get_spark()
    dataspec = dg.DataGenerator(
        spark, rows=50, partitions=8, randomSeedMethod="hash_fieldname"
    ).withSchema(worker_schema.WORKER_SCHEMA)

    dataspec = (
        dataspec.withColumnSpec("tr01flag", 1)
        .withColumnSpec("tr01count", 1)
        .withColumnSpec("jr01flag", 1)
        .withColumnSpec("jr03flag", 1)
        .withColumnSpec("jr16cat1", 1)
        .withColumnSpec("ql01achq2", 1)
        .withColumnSpec("ql01year2", 2009)
        .withColumnSpec("ql02achq3", 0)
        .withColumnSpec("ql34achqe", 1)
        .withColumnSpec("ql34yeare", 2010)
        .withColumnSpec("ql37achq", 3)
        .withColumnSpec("ql37year", 2021)
        .withColumnSpec("ql313app", 1)
        .withColumnSpec("ql313year", 2013)
    )

    df = dataspec.build()

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_prepared_locations_file_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "region", "services_offered"]
    # fmt: off
    rows = [
        ("1-1783948", "South East", ["Supported living service", "Acute services with overnight beds"]),
        ("1-1334987222", "South West", ["Domiciliary care service"]),
        ("1-348374832", "Merseyside", ["Extra Care housing services"]),
        ("1-683746776", "Merseyside", ["Doctors treatment service","Long term conditions services","Shared Lives"]),
        ("1-10478686 ", "London Senate", ["Community health care services - Nurses Agency only"]),
        ("1-10235302415", "South West", ["Urgent care services", "Supported living service"]),
        ("1-1060912125", "Yorkshire and The Humbler", ["Acute services with overnight beds"]),
        ("1-107095666", "Yorkshire and The Humbler", ["Specialist college service","Community based services for people who misuse substances","Urgent care services'"]),
        ("1-108369587", "South West", ["Specialist college service"]),
        ("1-10758359583", "Yorkshire and The Humbler", ["Mobile doctors service"]),
        ("1-108387554", "Yorkshire and The Humbler", ["Doctors treatment service", "Hospice services at home"]),
        ("1-10894414510", "Yorkshire and The Humbler", ["Care home service with nursing"]),
        ("1-108950835", "Merseyside", ["Care home service without nursing'"]),
        ("1-108967195", "Merseyside", ["Domiciliary care service"]),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_locationid_and_providerid_file(output_destination):
    spark = utils.get_spark()
    columns = ["providerId", "locationId", "other_cols"]

    rows = [
        ("1-000000001", "1-000000001", "other_data"),
        ("1-000000002", "1-000000002", "other_data"),
        ("1-000000002", "1-000000003", "other_data"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_duplicate_providerid_data_file(output_destination):
    spark = utils.get_spark()
    columns = [
        "providerId",
        "provider_name",
        "provider_mainphonenumber",
        "provider_postaladdressline1",
        "provider_postaladdresstowncity",
        "provider_postaladdresscounty",
        "provider_postalcode",
    ]
    rows = [
        ("1-000000001", "1", "2", "3", "4", "5", "6"),
        ("1-000000002", "2", "3", "4", "5", "6", "7"),
        ("1-000000002", "2", "3", "4", "5", "6", "7"),
        ("1-000000003", "3", "4", "5", "6", "7", "8"),
        ("1-000000003", "3", "4", "5", "6", "7", "8"),
        ("1-000000003", "3", "4", "5", "6", "7", "8"),
        ("1-000000004", "4", "5", "6", "7", "8", "9"),
        ("1-000000004", "4", "5", "6", "7", "8", "9"),
        ("1-000000004", "4", "5", "6", "7", "8", "9"),
        ("1-000000004", "4", "5", "6", "7", "8", "9"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_care_directory_locationid_file(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationId",
        "providerId",
        "type",
        "name",
        "registrationdate",
        "numberofbeds",
        "website",
        "postaladdressline1",
        "postaladdresstowncity",
        "postaladdresscounty",
        "region",
        "postalcode",
        "carehome",
        "mainphonenumber",
        "localauthority",
        "othercolumn",
    ]
    # fmt: off
    rows = [
        ("1-000000001", "1-000000001", "Social Care Org", "Name 1", date(2023, 3, 19), 5, "www.website.com", "1 rd", "Town", "County", "Region", "AB1 2CD", "Y", "07", "LA", "Other data",),
        ("1-000000002", "1-000000002", "Social Care Org", "Name 2", date(2023, 3, 19), 5, "www.website.com", "1 rd", "Town", "County", "Region", "AB1 2CD", "Y", "07", "LA", "Other data",),
        ("1-000000003", "1-000000002", "Social Care Org", "Name 3", date(2023, 3, 19), 5, "www.website.com", "1 rd", "Town", "County", "Region", "AB1 2CD", "Y", "07", "LA", "Other data",),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_multiple_boolean_columns(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationId",
        "Column A",
        "Column B",
        "Column C",
        "Column D",
        "Column-E",
    ]
    rows = [
        ("1-000000001", "Y", "Y", "Y", None, None),
        ("1-000000002", None, "Y", None, "Y", None),
        ("1-000000003", None, None, None, None, "Y"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            output_destination
        )

    return df


def generate_care_directory_registered_manager_name(output_destination):
    spark = utils.get_spark()
    register_manager_schema = StructType(
        fields=[
            StructField("locationId", StringType(), True),
            StructField("registered_manager_name", StringType(), True),
        ]
    )
    rows = [
        ("1-000000001", "Surname, Firstname"),
        ("1-000000002", "Surname, First Name"),
        ("1-000000003", None),
    ]
    df = spark.createDataFrame(data=rows, schema=register_manager_schema)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_care_directory_gac_service_types(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationId",
        "gacservicetypes",
    ]
    rows = [
        ("1-000000001", [["The name", "description"], ["The name 2", "description 2"]]),
        ("1-000000002", [["Another name", "Some other description"]]),
        ("1-000000003", []),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_care_directory_specialisms(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationId",
        "specialisms",
    ]
    rows = [
        ("1-000000001", [["The name"], ["The name 2"]]),
        ("1-000000002", [["Another name"]]),
        ("1-000000003", []),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df
