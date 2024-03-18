from datetime import date, datetime

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
    DateType,
)
import pyspark.sql.functions as F

from schemas import cqc_care_directory_schema
from utils import utils
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)


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
    columns = ["locationid", "workerid", "mainjrid", "import_date"]

    rows = [
        ("1-000000001", "100", 1, "20220101"),
        ("1-000000001", "101", 1, "20220101"),
        ("1-000000001", "102", 1, "20220101"),
        ("1-000000001", "103", 1, "20220101"),
        ("1-000000001", "104", 2, "20220101"),
        ("1-000000001", "105", 3, "20220101"),
        ("1-000000002", "106", 1, "20220101"),
        ("1-000000002", "107", 3, "20220101"),
        ("1-000000002", "108", 2, "20220101"),
        ("1-000000003", "109", 1, "20220101"),
        ("1-000000003", "110", 2, "20220101"),
        ("1-000000003", "111", 3, "20220101"),
        ("1-000000004", "112", 1, "20220101"),
        ("1-000000004", "113", 2, "20220101"),
        ("1-000000004", "114", 3, "20220101"),
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
    # fmt: off
    rows = [
        ("E01000001 : Area name 001A", "876", "767", "608", "18", "141", "15", "74", "4", "16"),
        ("E01000002 : Area name 001B", "830", "763", "630", "13", "120", "16", "45", "2", "4"),
        ("E01000003 : Area name 001C", "817", "678", "533", "26", "119", "22", "84", "20", "13"),
        ("E01000005 : Area name 001E", "467", "311", "222", "11", "78", "23", "77", "37", "19"),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            output_destination
        )

    return df


def generate_estimate_jobs_parquet(output_destination):
    spark = utils.get_spark()
    # fmt: off
    columns = ["locationid", "primary_service_type", "estimate_job_count", "snapshot_date", "run_year", "run_month", "run_day"]

    rows = [
        ("1-000000001", "Care home without nursing", 15.5, "20220101", "2022", "01", "02"),
        ("1-000000002", "Care home with nursing", 90.0, "20220101", "2022", "01", "02"),
        ("1-000000003", "Care home with nursing", 2.1, "20220101", "2022", "01", "02"),
        ("1-000000004", "non-residential", 25.3, "20220101", "2022", "01", "02"),
        ("1-000000005", "non-residential", 94.0, "20220101", "2022", "01", "02"),
    ]
    # fmt: on

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
        ("1-000000001", 1, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire & Humberside", "UB4 0EJ.", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000002", 2, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20190101"),
        ("1-000000003", 3, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20200101"),
        ("1-000000004", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "Yorkshire & Humberside", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000005", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000006", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "YO61 3FF", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000007", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000008", 6, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000009", 7, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20190101"),
        ("1-000000010", 8, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000011", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "HU21 0LS", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000012", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000013", 10, "location", "Social Care Org", "name of organisation", "Deregistered", "2011-02-15", "2015-01-01", "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000014", 11, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000015", 12, "location", "Not social care", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20200101"),
        ("1-000000001", 1, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000002", 2, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20210101"),
        ("1-000000003", 3, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000004", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000005", 4, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000006", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000007", 5, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "Yorkshire and The Humber", "TS20 2BI", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000008", 6, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000009", 7, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "Y", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service without nursing"}], "20210101"),
        ("1-000000010", 8, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000011", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000012", 9, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000013", 10, "location", "Social Care Org", "name of organisation", "Deregistered", "2011-02-15", "2015-01-01", "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000014", 11, "location", "Social Care Org", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", "OX29 9UB", "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
        ("1-000000015", 12, "location", "Not social care", "name of organisation", "Registered", "2011-02-15", None, "N", 50, "South East", None, "Y", "Rochester and Strood", "Medway", [{"name": "Nursing homes", "description": "Care home service with nursing"}], "20220101"),
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
        (1, "new provider name 1", "20220605"),
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
                "how_many_people_are_directly_employed"
                "_and_deliver_regulated_activities_at_"
                "your_service_as_part_of_their_daily_duties",
                IntegerType(),
                True,
            ),
            StructField("import_date", StringType(), True),
        ]
    )

    rows = [
        ("1-000000001", 20, "20220605"),
        ("1-000000002", 10, "20220605"),
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


def generate_ons_denormalised_data(output_destination):
    spark = utils.get_spark()
    # fmt: off
    schema = StructType([
        StructField(ONS.postcode, StringType(), False),
        StructField(ONS.cssr, StringType(), False),
        StructField(ONS.region, StringType(), False),
        StructField(ONS.sub_icb, StringType(), True),
        StructField(ONS.icb, StringType(), False),
        StructField(ONS.icb_region, StringType(), False),
        StructField(ONS.ccg, StringType(), False),
        StructField(ONS.latitude, StringType(), False),
        StructField(ONS.longitude, StringType(), False),
        StructField(ONS.imd_score, StringType(), False),
        StructField(ONS.lower_super_output_area_2011, StringType(), False),
        StructField(ONS.middle_super_output_area_2011, StringType(), False),
        StructField(ONS.rural_urban_indicator_2011, StringType(), False),
        StructField(ONS.lower_super_output_area_2021, StringType(), False),
        StructField(ONS.middle_super_output_area_2021, StringType(), False),
        StructField(ONS.westminster_parliamentary_consitituency, StringType(), False),
        StructField(ONS.year, StringType(), False),
        StructField(ONS.month, StringType(), False),
        StructField(ONS.day, StringType(), False),
        StructField(ONS.import_date, StringType(), False),
    ])
    rows = [
        ("SW100AA", "Ealing", "London", None, "North West London", "London", "NHS Ealing", "51.507582", "-.305451", "24623", "E01001386", "E02000268", "Urban major conurbation", "E01001386", "E02000268", "Ealing Central and Acton", "2022", "05", "01", "20220501"),
        ("SW10 0AB", "Ealing", "London", None, "North West London", "London", "NHS Ealing", "51.507582", "-.305451", "24623", "E01001386", "E02000268", "Urban major conurbation", "E01001386", "E02000268", "Ealing Central and Acton", "2022", "05", "01", "20220501"),
        ("SW100AD", "Ealing", "London", None, "South West London", "London", "NHS Ealing", "51.507582", "-.305451", "24623", "E01001386", "E02000268", "Urban major conurbation", "E01001386", "E02000268", "Ealing Central and Acton", "2022", "05", "01", "20220501")
    ]
    # fmt: on

    df = spark.createDataFrame(rows, schema)
    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)
    return df


def generate_ons_data(output_destination):
    spark = utils.get_spark()
    # fmt: off
    columns = ["pcd", "nhser", "year", "month", "day", "import_date"]
    rows = [
        ("SW9 0LL", "E40000003", "2021", "02", "01", "20210201"),
        ("BH1 1QZ", "E40000006", "2021", "02", "01", "20210201"),
        ("PO6 2EQ", "E40000005", "2021", "02", "01", "20210201"),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, columns)
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
        "parentid",
        "lastloggedin",
    ]
    # fmt: off
    rows = [
        ("1-000000001", "101", 14, 16, "20200101", "1", date(2021, 2, 1), 0, "201", date(2021, 2, 1),),
        ("1-000000002", "102", 76, 65, "20200101", "1", date(2021, 4, 1), 1, None, date(2021, 2, 1),),
        ("1-000000003", "103", 34, 34, "20200101", "2", date(2021, 3, 1), 0, "203", date(2021, 2, 1),),
        ("1-000000004", "104", 234, 265, "20190101", "2", date(2021, 4, 1), 0, None, date(2021, 2, 1),),
        ("1-000000005", "105", 62, 65, "20190101", "3", date(2021, 10, 1), 0, None, date(2021, 2, 1),),
        ("1-000000006", "106", 77, 77, "20190101", "3", date(2020, 3, 1), 1, None, date(2021, 2, 1),),
        ("1-000000007", "107", 51, 42, "20190101", " 3", date(2021, 5, 1), 0, None, date(2021, 5, 1),),
        ("1-000000008", "108", 36, 34, "20190101", "4", date(2021, 7, 1), 0, None, date(2021, 5, 1),),
        ("1-000000009", "109", 34, 32, "20190101", "5", date(2021, 12, 1), 0, None, date(2021, 5, 1),),
        ("1-0000000010", "110", 14, 20, "20190101", "6", date(2021, 3, 1), 0, None, date(2021, 5, 1),),
        ("1-000000001", "101", 14, 16, "20220101", "1", date(2021, 2, 1), 0, None, date(2021, 5, 1),),
        ("1-000000002", "102", 76, 65, "20220101", "1", date(2021, 4, 1), 1, None, date(2021, 5, 1),),
        ("1-000000003", "103", 34, 34, "20220101", "2", date(2021, 3, 1), 0, None, date(2021, 5, 1),),
        ("1-000000004", "104", 234, 265, "20220101", "2", date(2021, 4, 1), 0, None, date(2021, 5, 1),),
        ("1-000000005", "105", 62, 65, "20210101", "3", date(2021, 10, 1), 0, None, date(2021, 5, 1),),
        ("1-000000006", "106", 77, 77, "20210101", "3", date(2020, 3, 1), 1, None, date(2021, 5, 1),),
        ("1-000000007", "107", 51, 42, "20210101", "3", date(2021, 5, 1), 0, None, date(2021, 5, 1),),
        ("1-000000008", "108", 36, 34, "20210101", "4", date(2021, 7, 1), 0, None, date(2021, 5, 1),),
        ("1-000000009", "109", 34, 32, "20210101", "5", date(2021, 12, 1), 0, None, date(2021, 5, 1),),
        ("1-0000000010", "110", 14, 20, "20210101", "6", date(2021, 3, 1), 0, None, date(2021, 5, 1),),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ascwds_stayer_leaver_workplace_data(output_destination):
    spark = utils.get_spark()
    columns = ["establishmentid", "import_date", "wkrrecs", "mupddate", "other_column"]

    rows = [
        ("100", "20201231", 1, date(2020, 1, 1), "0"),
        ("101", "20210101", 1, date(2020, 1, 1), "0"),
        ("102", "20210101", 1, date(2020, 2, 1), "1"),
        ("103", "20210101", 1, date(2020, 3, 1), "0"),
        ("104", "20210101", 1, date(2020, 4, 1), "0"),
        ("105", "20210101", 1, date(2020, 5, 1), "0"),
        ("106", "20210101", 5, date(2020, 6, 1), "1"),
        ("107", "20210101", 5, date(2020, 7, 1), "0"),
        ("108", "20210101", 5, date(2020, 8, 1), "0"),
        ("109", "20210101", 10, date(2020, 9, 1), "0"),
        ("110", "20210101", 0, date(2020, 10, 1), "0"),
        ("111", "20210101", 5, date(2020, 11, 1), "0"),
        ("112", "20210101", 10, date(2020, 12, 1), "1"),
        ("113", "20210101", None, date(2020, 12, 1), "1"),
        ("114", "20210601", 10, date(2021, 6, 1), "0"),
        ("106", "20220101", 10, date(2021, 12, 1), "1"),
        ("107", "20220101", 10, date(2021, 12, 1), "0"),
        ("108", "20220101", 10, date(2021, 11, 1), "0"),
        ("109", "20220101", 10, date(2021, 11, 1), "0"),
        ("110", "20220101", 10, date(2021, 10, 1), "0"),
        ("111", "20220101", 10, date(2021, 10, 1), "0"),
        ("112", "20220101", 10, date(2021, 5, 1), "1"),
        ("113", "20220101", 10, date(2021, 12, 1), "0"),
        ("114", "20220101", 10, date(2021, 12, 1), "0"),
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
        ("1-10000000001", "01/12/2020", "Y", "Location 1", "Social Care Org", "7123456789", "*", "", 1, "Region 6", "LA 9", "7 main road", "", "", "", "AB4 5CD", "BD001", "BRAND NAME 1", "1-10000000001", "Provider 1", "", "www.website1.com", "1 ave", "", "", "", "AB1 2CD", "Surname1, First1", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y",),
        ("1-10000000002", "01/11/2021", "N", "Location 2", "Social Care Org", "7123456789", "Surname1, First1", "www.website.com", 0, "Region 4", "LA 8", "13 main road", "", "", "", "AB7 7CD", "-", "-", "1-10000000002", "Provider 2", "7123456789", "", "2 ave", "", "", "", "AB3 4CD", "Surname2, First2", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "",),
        ("1-10000000003", "07/10/2022", "Y", "Location 3", "Social Care Org", "7123456789", "Surname1, First1", "", 5, "Region 5", "LA 6", "6 main road", "", "", "", "AB6 3CD", "-", "-", "1-10000000002", "Provider 2", "7123456789", "", "2 ave", "", "", "", "AB3 4CD", "Surname2, First2", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "", "", "", "", "", "Y", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "",),
        ("1-10000000004", "10/09/2023", "N", "Location 4", "Social Care Org", "7123456789", "Surname2, First2", "www.website.com", 0, "Region 1", "LA 5", "95 main road", "", "", "", "AB8 4CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "", "Y", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "",),
        ("1-10000000005", "13/08/2024", "Y", "Location 5", "Social Care Org", "7123456789", "Surname3, First3", "", 10, "Region 4", "LA 4", "5 main road", "", "", "", "AB2 2CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "Y", "", "", "", "", "Y", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "",),
        ("1-10000000006", "16/07/2025", "N", "Location 6", "Social Care Org", "7123456789", "Surname4, First4", "www.website.com", 0, "Region 1", "LA 2", "2 main road", "", "", "", "AB7 2CD", "-", "-", "1-10000000003", "Provider 3", "7123456788", "www.website2.com", "3 ave", "", "", "", "AB5 6CD", "*", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "Y", "", "", "Y", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "",),
        ("1-10000000007", "19/06/2026", "Y", "Location 7", "Social Care Org", "7123456789", "Surname5, First5", "", 50, "Region 4", "LA 1", "33 main road", "", "", "", "AB3 1CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "", "", "", "", "", "",),
        ("1-10000000008", "22/05/2017", "N", "Location 8", "Social Care Org", "", "Surname6, First6", "www.website.com", 0, "Region 6", "LA 8", "90 main road", "", "", "", "AB3 5CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "", "", "", "", "", "", "",),
        ("1-10000000009", "25/04/2018", "Y", "Location 9", "Social Care Org", "7123456789", "Surname7, First7", "", 100, "Region 4", "LA 10", "61 main road", "", "", "", "AB8 1CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "", "Y", "Y", "Y", "", "", "", "", "", "", "", "",),
        ("1-10000000010", "28/03/2029", "N", "Location 10", "Social Care Org", "7123456789", "*", "www.website.com", 0, "Region 2", "LA 6", "56 main road", "", "", "", "AB4 4CD", "BD002", "BRAND NAME 2", "1-10000000004", "Provider 4", "7123456787", "", "4 ave", "", "", "", "AB7 8CD", "Surname4, First4", "", "", "", "", "", "", "", "", "", "Y", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "Y", "", "Y", "Y", "", "", "", "", "", "", "", "", "", "",),
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


def generate_version_0_ascwds_worker_file(output_destination):
    spark = utils.get_spark()
    # version 0.0.1
    schema = StructType(
        [
            StructField("workerid", StringType(), True),
            StructField("establishmentid", StringType(), True),
            StructField("emplstat", StringType(), True),
            StructField("zerohours", StringType(), True),
            StructField("salaryint", StringType(), True),
            StructField("hrlyrate", StringType(), True),
            StructField("distwrkk", StringType(), True),
            StructField("dayssick", StringType(), True),
            StructField("conthrs", StringType(), True),
            StructField("averagehours", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("import_date", StringType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("tr01flag", StringType(), True),
            StructField("tr01latestdate", StringType(), True),
            StructField("tr01count", StringType(), True),
            StructField("tr01ac", StringType(), True),
            StructField("tr01nac", StringType(), True),
            StructField("tr01dn", StringType(), True),
            StructField("jr01flag", StringType(), True),
            StructField("jr03flag", StringType(), True),
            StructField("jr16cat1", StringType(), True),
            StructField("ql01achq2", StringType(), True),
            StructField("ql01year2", StringType(), True),
            StructField("ql02achq3", StringType(), True),
            StructField("ql34achqe", StringType(), True),
            StructField("ql34yeare", StringType(), True),
            StructField("ql37achq", StringType(), True),
            StructField("ql37year", StringType(), True),
            StructField("ql313app", StringType(), True),
            StructField("ql313year", StringType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("855821", "10101", "190", "1", "250", "7.68", "1.255169808", "10", "0", "35", "15462.34", "20200601", "2020", "06", "01", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None, "1", "2013"),
        ("1109430", "34567", "190", "0", "252", "8.11", "2.028776943", "10", "37.5", "0", "", "20200601", "2020", "06", "01", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None, "1", "2013"),
        ("1109429", "34567", "191", "1", "252", "7.68", "24.96731587", "10", "0", "35", "", "20200501", "2020", "05", "01", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None, "1", "2013"),
        ("855824", "34567", "191", "0", "250", "7.68", "0", "10", "0", "35", "13260", "20200501", "2020", "05", "01", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None, "1", "2013"),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, schema)

    df = df.withColumn("version", F.lit("0.0.1"))
    df = df.withColumn("old_unused_column", F.lit("a"))

    if output_destination:
        df.coalesce(1).write.partitionBy("version").mode("append").parquet(
            output_destination
        )

    return df, schema


def generate_version_1_ascwds_worker_file(output_destination):
    spark = utils.get_spark()

    schema = StructType(
        fields=[
            StructField("workerid", StringType(), True),
            StructField("establishmentid", StringType(), True),
            StructField("tr01flag", StringType(), True),
            StructField("tr01latestdate", StringType(), True),
            StructField("tr01count", StringType(), True),
            StructField("tr01ac", StringType(), True),
            StructField("tr01nac", StringType(), True),
            StructField("tr01dn", StringType(), True),
            StructField("jr01flag", StringType(), True),
            StructField("jr03flag", StringType(), True),
            StructField("jr16cat1", StringType(), True),
            StructField("ql01achq2", StringType(), True),
            StructField("ql01year2", StringType(), True),
            StructField("ql02achq3", StringType(), True),
            StructField("ql34achqe", StringType(), True),
            StructField("ql34yeare", StringType(), True),
            StructField("ql37achq", StringType(), True),
            StructField("ql37year", StringType(), True),
            StructField("ql313app", StringType(), True),
            StructField("ql313year", StringType(), True),
            StructField("distwrkk", StringType(), True),
            StructField("dayssick", StringType(), True),
            StructField("previous_pay", StringType(), True),
            StructField("emplstat", StringType(), True),
            StructField("conthrs", StringType(), True),
            StructField("averagehours", StringType(), True),
            StructField("zerohours", StringType(), True),
            StructField("salaryint", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("hrlyrate", StringType(), True),
            StructField("import_date", StringType(), True),
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("savedate", TimestampType(), True),
            StructField("previous_mainjrid", StringType(), True),
            StructField("dayssick_savedate", TimestampType(), True),
            StructField("pay_savedate", TimestampType(), True),
            StructField("flujab2020", StringType(), True),
            StructField("derivedfrom_hasbulkuploaded", StringType(), True),
        ]
    )

    # fmt:off
    rows = [
        (
            "855823", "12345", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None,
            "1", "2013", "30.3", "19.0", "0.5", "190", "8.5", "26.5", "1", "250", "5200", "100.5", "20220101", "2022", "01", "01",
            datetime(2018, 2, 3), "h346736", datetime(2020, 4, 1), datetime(2019, 1, 1), "yes", "no"
        ),
        (
            "855819", "34567", "1", "2017-06-15", "10", "0", "0", "0", "1", "1", "1", "1", "2009", "0", "1", "2020", "3", None,
            "1", "2013", "30.3", "19.0", "0.5", "190", "8.5", "26.5", "1", "250", "5200", "100.5", "20210101", "2022", "01", "01",
            datetime(2018, 2, 3), "h346736", datetime(2020, 4, 1), datetime(2019, 1, 1), "yes", "no"
        )
    ]
    # fmt:on

    df = spark.createDataFrame(rows, schema)

    df = df.withColumn("version", F.lit("1.0.0"))
    df = df.withColumn("unused_column", F.lit("d"))

    if output_destination:
        df.coalesce(1).write.partitionBy("version").mode("append").parquet(
            output_destination
        )

    return df, schema


def generate_flexible_worker_file_hours_worked(
    emplstat, zerohours, averagehours, conthrs
):
    spark = utils.get_spark()
    columns = [
        "emplstat",
        "zerohours",
        "averagehours",
        "conthrs",
    ]
    rows = [(emplstat, zerohours, averagehours, conthrs)]

    df = spark.createDataFrame(rows, columns)

    return df


def generate_flexible_worker_file_hourly_rate(salary, salaryint, hrlyrate, hrs_worked):
    spark = utils.get_spark()
    columns = ["salary", "salaryint", "hrlyrate", "hrs_worked"]
    rows = [(salary, salaryint, hrlyrate, hrs_worked)]

    df = spark.createDataFrame(rows, columns)

    return df


def generate_location_with_ons_parquet(output_destination):
    spark = utils.get_spark()
    columns = [
        "establishmentid",
        "postal_code",
        "ons_region",
        "nhs_england_region",
        "country",
        "lsoa",
        "msoa",
        "clinical_commisioning_group",
        "rural_urban_indicator",
        "oslaua",
        "ons_import_date",
        "snapshot_date",
        "snapshot_year",
        "snapshot_month",
        "snapshot_day",
    ]

    # fmt:off
    rows = [
        (
            "12345", "AB0 7CD", "South West", "London", "England", {"year_2011": "Tendring 018A"}, {"year_2011": "City of London 001"},
            "NHS Barnsley CCG", {"year_2011": "B1"}, "Kensington and Chelsea", "20210101", "20220102", "2022", "01", "02",
        ),
        (
            "12345", "AB0 7CD", "South West", "London", "England", {"year_2011": "Tendring 018A"}, {"year_2011": "City of London 001"},
            "NHS Barnsley CCG", {"year_2011": "B1"}, "Kensington and Chelsea", "20210101", "20200506", "2020", "05", "06",
        ),
        (
            "12345", "AB0 7CD", "South West", "London", "England", {"year_2011": "Tendring 018A"}, {"year_2011": "City of London 001"},
            "NHS Barnsley CCG", {"year_2011": "B1"}, "Kensington and Chelsea", "20210101", "20220202", "2022", "02", "02",
        ),
        (
            "10101", "EF0 7GH", "South East", "London", "England", {"year_2011": "Tendring 128A"}, {"year_2011": "City of London 003"},
            "NHS Barnsley CCG", {"year_2011": "B1"}, "Kensington and Chelsea", "20210103", "20210203", "2021", "02", "03",
        ),
        (
            "10000", "EF0 7GH", "South East", "London", "England", {"year_2011": "Tendring 128A"}, {"year_2011": "City of London 003"},
            "NHS Barnsley CCG", {"year_2011": "B1"}, "Kensington and Chelsea", "20210103", "20210104", "2021", "01", "04",
        ),
    ]
    # fmt:on

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_location_features_file_parquet(output_destination=None):
    spark = utils.get_spark()
    # fmt: off
    feature_columns = ["locationid", "job_count", "carehome", "region", "snapshot_year", "snapshot_month", "snapshot_day", "snapshot_date"]

    feature_rows = [
        ("1-000000001", 10, "Y", "South West", "2022", "02", "28", "2022-03-29"),
        ("1-000000002", 10, "N", "Merseyside", "2022", "02", "28", "2022-03-29"),
        ("1-000000003", 20, None, "Merseyside", "2022", "02", "28", "2022-03-29"),
        ("1-000000004", 10, "N", None, "2022", "02", "28", "2022-03-29"),
    ]
    # fmt: on
    df = spark.createDataFrame(
        feature_rows,
        schema=feature_columns,
    )
    if output_destination:
        df.write.mode("overwrite").partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(output_destination)
    return df


def generate_prepared_locations_preclean_file_parquet(
    output_destination=None, partitions=["2022", "03", "08"], append=False
):
    spark = utils.get_spark()
    schema = StructType(
        [
            StructField("locationId", StringType(), True),
            StructField("snapshot_date", StringType(), True),
            StructField("ons_region", StringType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField(
                "services_offered",
                ArrayType(
                    StringType(),
                ),
                True,
            ),
            StructField("primary_service_type", StringType(), True),
            StructField("people_directly_employed", IntegerType(), True),
            StructField("total_staff", IntegerType(), True),
            StructField("local_authority", StringType(), True),
            StructField("snapshot_year", StringType(), True),
            StructField("snapshot_month", StringType(), True),
            StructField("snapshot_day", StringType(), True),
            StructField("carehome", StringType(), True),
            StructField("cqc_sector", StringType(), True),
            StructField("rural_urban_indicator", StringType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("registration_status", StringType(), True),
        ]
    )

    # fmt: off
    rows = [
        ("1-1783948", "20220201", "South East", 0, ["Domiciliary care service"], "non-residential", 5, None, "Surrey", partitions[0], partitions[1], partitions[2], "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", 1, "Registered"),
        ("1-1783948", "20220101", "South East", 0, ["Domiciliary care service"], "non-residential", 5, 67, "Surrey", partitions[0], partitions[1], partitions[2], "N", "Independent", "Rural hamlet and isolated dwellings in a sparse setting", 1, "Registered"),
        ("1-348374832", "20220112", "Merseyside", 0, ["Extra Care housing services"], "non-residential", None, 34, "Gloucestershire", partitions[0], partitions[1], partitions[2], "N", "Local authority", "Rural hamlet and isolated dwellings", 1, "Registered"),
        ("1-683746776", "20220101", "Merseyside", 0, ["Doctors treatment service", "Long term conditions services", "Shared Lives"], "non-residential", 34, None, "Gloucestershire", partitions[0], partitions[1], partitions[2], "N", "Local authority", "Rural hamlet and isolated dwellings", 1, "Registered"),
        ("1-10478686", "20220101", "London Senate", 0, ["Community health care services - Nurses Agency only"], "non-residential", None, None, "Surrey", partitions[0], partitions[1], partitions[2], "N", "", "Rural hamlet and isolated dwellings", 1, "Registered"),
        ("1-10235302415", "20220112", "South West", 0, ["Urgent care services", "Supported living service"], "non-residential", 17, None, "Surrey", partitions[0], partitions[1], partitions[2], "N", "Independent", "Rural hamlet and isolated dwellings", 1, "Registered"),
        ("1-1060912125", "20220112", "Yorkshire and The Humbler", 0, ["Hospice services at home"], "non-residential", 34, None, "Surrey", partitions[0], partitions[1], partitions[2], "N", "Independent", "Rural hamlet and isolated dwellings", 1, "Registered"),
        ("1-107095666", "20220301", "Yorkshire and The Humbler", 0, ["Specialist college service", "Community based services for people who misuse substances", "Urgent care services'"], "non-residential", 34, None, "Lewisham", partitions[0], partitions[1], partitions[2], "N", "Independent", "Urban city and town", 1, "Registered"),
        ("1-108369587", "20220308", "South West", 0, ["Specialist college service"], "non-residential", 15, None, "Lewisham", partitions[0], partitions[1], partitions[2], "N", "Independent", "Rural town and fringe in a sparse setting", 1, "Registered"),
        ("1-10758359583", "20220308", None, 0, ["Mobile doctors service"], "non-residential", 17, None, "Lewisham", partitions[0], partitions[1], partitions[2], "N", "Local authority", "Urban city and town", 1, "Registered"),
        ("1-000000001", "20220308", "Yorkshire and The Humbler", 67, ["Care home service with nursing"], "Care home with nursing", None, None, "Lewisham", partitions[0], partitions[1], partitions[2], "Y", "Local authority", "Urban city and town", 1, "Registered"),
        ("1-10894414510", "20220308", "Yorkshire and The Humbler", 10, ["Care home service with nursing"], "Care home with nursing", 0, 25, "Lewisham", partitions[0], partitions[1], partitions[2], "Y", "Independent", "Urban city and town", 1, "Registered"),
        ("1-108950835", "20220315", "Merseyside", 20, ["Care home service without nursing"], "Care home without nursing", 23, None, "Lewisham", partitions[0], partitions[1], partitions[2], "Y", "", "Urban city and town", 1, "Registered"),
        ("1-108967195", "20220422", "(pseudo) Wales", 0, ["Supported living service", "Acute services with overnight beds"], "non-residential", 11, None, "Lewisham", partitions[0], partitions[1], partitions[2], "N", "Independent", "Urban city and town", 1, "Deregistered"),
    ]
    # fmt: on

    df = spark.createDataFrame(rows, schema=schema)

    df = df.withColumn("snapshot_date", F.to_date(df.snapshot_date, "yyyyMMdd"))
    if append:
        mode = "append"
    else:
        mode = "overwrite"
    if output_destination:
        df.write.mode(mode).partitionBy(
            "snapshot_year", "snapshot_month", "snapshot_day"
        ).parquet(output_destination)

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


def generate_ascwds_stayer_leaver_worker_data(output_destination):
    spark = utils.get_spark()
    columns = [
        "establishmentid",
        "workerid",
        "emplstat",
        "import_date",
    ]

    rows = [
        ("108", "1", "190", "20210101"),
        ("108", "2", "190", "20210101"),
        ("108", "3", "190", "20210101"),
        ("108", "4", "190", "20210101"),
        ("108", "5", "190", "20210101"),
        ("109", "6", "190", "20210101"),
        ("109", "7", "190", "20210101"),
        ("109", "8", "190", "20210101"),
        ("109", "9", "190", "20210101"),
        ("109", "10", "190", "20210101"),
        ("109", "11", "190", "20210101"),
        ("109", "12", "190", "20210101"),
        ("109", "13", "190", "20210101"),
        ("109", "14", "190", "20210101"),
        ("109", "15", "190", "20210101"),
        ("110", "16", "190", "20210101"),
        ("111", "17", "190", "20210101"),
        ("111", "18", "190", "20210101"),
        ("111", "19", "190", "20210101"),
        ("111", "20", "191", "20210101"),
        ("111", "21", "191", "20210101"),
        ("111", "22", "192", "20210101"),
        ("113", "24", "190", "20210101"),
        ("108", "1", "190", "20220101"),
        ("108", "3", "190", "20220101"),
        ("108", "5", "190", "20220101"),
        ("109", "7", "190", "20220101"),
        ("109", "9", "190", "20220101"),
        ("109", "11", "190", "20220101"),
        ("109", "13", "190", "20220101"),
        ("109", "15", "190", "20220101"),
        ("111", "17", "190", "20220101"),
        ("111", "19", "190", "20220101"),
        ("111", "21", "190", "20220101"),
        ("111", "22", "190", "20220101"),
        ("112", "23", "190", "20220101"),
        ("113", "24", "190", "20220101"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_filtered_workplaces(output_destination):
    spark = utils.get_spark()

    df = spark.createDataFrame(["108", "109", "111"], "string").toDF("establishmentid")

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


def generate_workplace_import_dates(output_destination):
    spark = utils.get_spark()
    workplace_df = spark.createDataFrame(
        [
            ("20220601",),
            ("20220101",),
            ("20210202",),
            ("20201225",),
        ],
        ["import_date"],
    )

    if output_destination:
        workplace_df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return workplace_df


def generate_worker_import_dates(output_destination):
    spark = utils.get_spark()
    worker_df = spark.createDataFrame(
        [
            ("20220101",),
            ("20211212",),
            ("20210101",),
            ("20201225",),
        ],
        ["import_date"],
    )

    if output_destination:
        worker_df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return worker_df


def generate_care_home_jobs_per_bed_filter_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("snapshot_date", StringType(), True),
            StructField("primary_service_type", StringType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count_unfiltered", DoubleType(), True),
        ]
    )

    # fmt: off
    rows = [
        ("01", "2023-01-01", "Care home with nursing", 25, 1.0),
        ("02", "2023-01-01", "Care home with nursing", 25, 2.0),
        ("03", "2023-01-01", "Care home with nursing", 25, 3.0),
        ("04", "2023-01-01", "Care home with nursing", 25, 4.0),
        ("05", "2023-01-01", "Care home with nursing", 25, 5.0),
        ("06", "2023-01-01", "Care home with nursing", 25, 6.0),
        ("07", "2023-01-01", "Care home with nursing", 25, 7.0),
        ("08", "2023-01-01", "Care home with nursing", 25, 8.0),
        ("09", "2023-01-01", "Care home with nursing", 25, 9.0),
        ("10", "2023-01-01", "Care home with nursing", 25, 10.0),
        ("11", "2023-01-01", "Care home with nursing", 25, 11.0),
        ("12", "2023-01-01", "Care home with nursing", 25, 12.0),
        ("13", "2023-01-01", "Care home with nursing", 25, 13.0),
        ("14", "2023-01-01", "Care home with nursing", 25, 14.0),
        ("15", "2023-01-01", "Care home with nursing", 25, 15.0),
        ("16", "2023-01-01", "Care home with nursing", 25, 16.0),
        ("17", "2023-01-01", "Care home with nursing", 25, 17.0),
        ("18", "2023-01-01", "Care home with nursing", 25, 18.0),
        ("19", "2023-01-01", "Care home with nursing", 25, 19.0),
        ("20", "2023-01-01", "Care home without nursing", 25, 20.0),
        ("21", "2023-01-01", "Care home without nursing", 25, 21.0),
        ("22", "2023-01-01", "Care home without nursing", 25, 22.0),
        ("23", "2023-01-01", "Care home without nursing", 25, 23.0),
        ("24", "2023-01-01", "Care home without nursing", 25, 24.0),
        ("25", "2023-01-01", "Care home without nursing", 25, 25.0),
        ("26", "2023-01-01", "Care home without nursing", 25, 26.0),
        ("27", "2023-01-01", "Care home without nursing", 25, 27.0),
        ("28", "2023-01-01", "Care home without nursing", 25, 28.0),
        ("29", "2023-01-01", "Care home without nursing", 25, 29.0),
        ("30", "2023-01-01", "Care home without nursing", 25, 30.0),
        ("31", "2023-01-01", "Care home without nursing", 25, 31.0),
        ("32", "2023-01-01", "Care home without nursing", 25, 32.0),
        ("33", "2023-01-01", "Care home without nursing", 25, 33.0),
        ("34", "2023-01-01", "Care home without nursing", 25, 34.0),
        ("35", "2023-01-01", "Care home without nursing", 25, 35.0),
        ("36", "2023-01-01", "Care home without nursing", 25, 36.0),
        ("37", "2023-01-01", "Care home without nursing", 25, 37.0),
        ("38", "2023-01-01", "Care home without nursing", 25, 38.0),
        ("39", "2023-01-01", "Care home without nursing", 25, 39.0),
        ("40", "2023-01-01", "Care home without nursing", 25, 40.0),
        ("41", "2023-01-01", "Care home with nursing", 25, None),
        ("42", "2023-01-01", "Care home with nursing", None, 42.0),
        ("43", "2023-01-01", "Any other service", 25, 43.0),
        ("44", "2023-01-01", "Any other service", None, 44.0),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_input_data_for_primary_service_rolling_average():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, 4.0, "non-residential"),
        ("1-000000002", "2023-01-01", 1672531200, 6.0, "non-residential"),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, "non-residential"),
        ("1-000000004", "2023-03-01", 1677628800, 30.0, "non-residential"),
        ("1-000000005", "2023-04-01", 1680303600, 40.0, "non-residential"),
        ("1-000000006", "2023-01-01", 1672531200, None, "non-residential"),
        ("1-000000007", "2023-02-01", 1675209600, None, "non-residential"),
        ("1-000000008", "2023-03-01", 1677628800, None, "non-residential"),
        ("1-000000011", "2023-01-01", 1672531200, 14.0, "Care home with nursing"),
        ("1-000000012", "2023-01-01", 1672531200, 16.0, "Care home with nursing"),
        ("1-000000013", "2023-02-01", 1675209600, 120.0, "Care home with nursing"),
        ("1-000000014", "2023-03-01", 1677628800, 131.0, "Care home with nursing"),
        ("1-000000015", "2023-04-01", 1680303600, 142.0, "Care home with nursing"),
        ("1-000000016", "2023-01-01", 1672531200, None, "Care home with nursing"),
        ("1-000000017", "2023-02-01", 1675209600, None, "Care home with nursing"),
        ("1-000000018", "2023-03-01", 1677628800, None, "Care home with nursing"),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_calculating_job_count_sum_and_count():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", 1672531200, 4.0, "non-residential"),
        ("1-000000002", 1672531200, 6.0, "non-residential"),
        ("1-000000003", 1675209600, 20.0, "non-residential"),
        ("1-000000004", 1677628800, 30.0, "non-residential"),
        ("1-000000005", 1680303600, 40.0, "non-residential"),
        ("1-000000011", 1672531200, 14.0, "Care home with nursing"),
        ("1-000000012", 1672531200, 16.0, "Care home with nursing"),
        ("1-000000013", 1675209600, 120.0, "Care home with nursing"),
        ("1-000000014", 1677628800, 131.0, "Care home with nursing"),
        ("1-000000015", 1680303600, 142.0, "Care home with nursing"),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_df_for_calculating_rolling_sum():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("primary_service_type", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("col_to_sum", StringType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("service_1", 86400, 10),
        ("service_1", 172800, 12),
        ("service_1", 259200, 15),
        ("service_1", 345600, 17),
        ("service_1", 432000, 20),
        ("service_2", 86400, 10),
        ("service_2", 172800, 11),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_rolling_average_dummy_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("other_col", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("primary_service_type", StringType(), False),
            StructField("rolling_average_model", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("random_data", 1672531200, "non-residential", 44.24),
        ("random_data", 1680303600, "Care home with nursing", 25.1),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_calculating_rolling_average_column():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("primary_service_type", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("count_of_job_count", IntegerType(), True),
            StructField("sum_of_job_count", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("Care home with nursing", 1672531200, 2, 30.0),
        ("Care home with nursing", 1675209600, 1, 120.0),
        ("Care home with nursing", 1677628800, 1, 131.0),
        ("Care home with nursing", 1680303600, 1, 142.0),
        ("non-residential", 1672531200, 2, 10.0),
        ("non-residential", 1675209600, 1, 20.0),
        ("non-residential", 1677628800, 1, 30.0),
        ("non-residential", 1680303600, 1, 40.0),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_interpolation_model():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("estimate_job_count", DoubleType(), True),
            StructField("estimate_job_count_source", StringType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, None, None, None),
        ("1-000000001", "2023-01-02", 1672617600, 30.0, 30.0, "ascwds_job_count"),
        ("1-000000001", "2023-01-03", 1672704000, None, None, None),
        ("1-000000002", "2023-01-01", 1672531200, None, None, None),
        ("1-000000002", "2023-01-03", 1672704000, 4.0, 4.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-05", 1672876800, None, None, None),
        ("1-000000002", "2023-01-07", 1673049600, 5.0, 5.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-09", 1673222400, 5.0, 5.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-11", 1673395200, None, None, None),
        ("1-000000002", "2023-01-13", 1673568000, None, None, None),
        ("1-000000002", "2023-01-15", 1673740800, 20.0, 20.0, "ascwds_job_count"),
        ("1-000000002", "2023-01-17", 1673913600, None, 21.0, "other_source"),
        ("1-000000002", "2023-01-19", 1674086400, None, None, None),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_calculating_first_and_last_submission_date_per_location():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
        ]
    )
    input_rows = [
        ("1-000000001", 1672617600, 1.0),
        ("1-000000002", 1672704000, 1.0),
        ("1-000000002", 1673049600, 1.0),
        ("1-000000002", 1673222400, 1.0),
    ]
    df = spark.createDataFrame(input_rows, schema=schema)

    return df


def generate_data_for_exploding_dates_into_timeseries_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("first_submission_time", LongType(), False),
            StructField("last_submission_time", LongType(), True),
        ]
    )
    input_rows = [
        ("1-000000001", 1672617600, 1672617600),
        ("1-000000002", 1672704000, 1673049600),
    ]
    df = spark.createDataFrame(input_rows, schema=schema)

    return df


def generate_data_for_merge_known_values_with_exploded_dates_exploded_timeseries_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("unix_time", LongType(), False),
        ]
    )
    input_rows = [
        ("1-000000001", 1672617600),
        ("1-000000002", 1672704000),
        ("1-000000002", 1672790400),
        ("1-000000002", 1672876800),
        ("1-000000003", 1672790400),
    ]
    df = spark.createDataFrame(input_rows, schema=schema)

    return df


def generate_data_for_merge_known_values_with_exploded_dates_known_ascwds_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
        ]
    )
    input_rows = [
        ("1-000000002", 1672704000, 1.0),
        ("1-000000002", 1672876800, 2.5),
        ("1-000000003", 1672790400, 15.0),
    ]
    df = spark.createDataFrame(input_rows, schema=schema)

    return df


def generate_data_for_interpolating_values_for_all_dates_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), True),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("job_count_unix_time", LongType(), True),
        ]
    )
    input_rows = [
        ("1-000000001", 1, 30.0, 1),
        ("1-000000002", 1, 4.0, 1),
        ("1-000000002", 2, None, None),
        ("1-000000002", 3, 5.0, 3),
        ("1-000000003", 2, 5.0, 2),
        ("1-000000003", 3, None, None),
        ("1-000000003", 4, None, None),
        ("1-000000003", 5, 8.5, 5),
    ]
    df = spark.createDataFrame(input_rows, schema=schema)

    return df


def generate_data_for_extrapolation_model():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
            StructField("estimate_job_count", DoubleType(), True),
            StructField("estimate_job_count_source", StringType(), True),
            StructField("rolling_average_model", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, 15.0, "Care home with nursing", None, None, 15.0),
        ("1-000000001", "2023-02-01", 1675209600, None, "Care home with nursing", None, None, 15.1),
        ("1-000000001", "2023-03-01", 1677628800, 30.0, "Care home with nursing", 30.0, "already_populated", 15.2),
        ("1-000000002", "2023-01-01", 1672531200, 4.0, "non-residential", None, None, 50.3),
        ("1-000000002", "2023-02-01", 1675209600, None, "non-residential", None, None, 50.5),
        ("1-000000002", "2023-03-01", 1677628800, None, "non-residential", 5.0, "already_populated", 50.7),
        ("1-000000002", "2023-04-01", 1680303600, None, "non-residential", None, None, 50.1),
        ("1-000000003", "2023-01-01", 1672531200, None, "non-residential", None, None, 50.3),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, "non-residential", None, None, 50.5),
        ("1-000000003", "2023-03-01", 1677628800, None, "non-residential", 30.0, "already_populated", 50.7),
        ("1-000000004", "2023-03-01", 1677628800, None, "non-residential", None, None, 50.7),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_extrapolation_location_filtering_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("primary_service_type", StringType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 15.0, "Care home with nursing"),
        ("1-000000002", "2023-01-01", None, "non-residential"),
        ("1-000000002", "2023-02-01", None, "non-residential"),
        ("1-000000003", "2023-01-01", 20.0, "non-residential"),
        ("1-000000003", "2023-02-01", None, "non-residential"),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_job_count_and_rolling_average_first_and_last_submissions_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("rolling_average_model", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, None, 12.0),
        ("1-000000001", "2023-02-01", 1675209600, 5.0, 15.0),
        ("1-000000001", "2023-03-01", 1677628800, None, 18.0),
        ("1-000000002", "2023-01-01", 1672531200, 4.0, 12.0),
        ("1-000000002", "2023-02-01", 1675209600, 6.0, 15.0),
        ("1-000000002", "2023-03-01", 1677628800, None, 18.0),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_adding_extrapolated_values_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("job_count", DoubleType(), True),
            StructField("rolling_average_model", DoubleType(), True),
            StructField("first_submission_time", LongType(), False),
            StructField("last_submission_time", LongType(), False),
            StructField("first_job_count", DoubleType(), True),
            StructField("first_rolling_average", DoubleType(), True),
            StructField("last_job_count", DoubleType(), True),
            StructField("last_rolling_average", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200, 15.0, 5.0, 1672531200, 1677628800, 15.0, 5.0, 30.0, 5.0),
        ("1-000000001", "2023-02-01", 1675209600, None, 5.0, 1672531200, 1677628800, 15.0, 5.0, 30.0, 5.0),
        ("1-000000001", "2023-03-01", 1677628800, 30.0, 5.0, 1672531200, 1677628800, 15.0, 5.0, 30.0, 5.0),
        ("1-000000002", "2023-01-01", 1672531200, 40.0, 1.0, 1672531200, 1672531200, 40.0, 1.0, 40.0, 1.0),
        ("1-000000002", "2023-02-01", 1675209600, None, 1.5, 1672531200, 1672531200, 40.0, 1.0, 40.0, 1.0),
        ("1-000000002", "2023-03-01", 1677628800, None, 0.5, 1672531200, 1672531200, 40.0, 1.0, 40.0, 1.0),
        ("1-000000003", "2023-01-01", 1672531200, None, 1.0, 1675209600, 1675209600, 20.0, 1.7, 20.0, 1.7),
        ("1-000000003", "2023-02-01", 1675209600, 20.0, 1.7, 1675209600, 1675209600, 20.0, 1.7, 20.0, 1.7),
        ("1-000000003", "2023-03-01", 1677628800, None, 2.0, 1675209600, 1675209600, 20.0, 1.7, 20.0, 1.7),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_adding_extrapolated_values_to_be_added_into_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1672531200),
        ("1-000000001", "2023-02-01", 1675209600),
        ("1-000000001", "2023-03-01", 1677628800),
        ("1-000000002", "2023-01-01", 1672531200),
        ("1-000000002", "2023-02-01", 1675209600),
        ("1-000000002", "2023-03-01", 1677628800),
        ("1-000000003", "2023-01-01", 1672531200),
        ("1-000000003", "2023-02-01", 1675209600),
        ("1-000000003", "2023-03-01", 1677628800),
        ("1-000000003", "2023-04-01", 1680303600),
        ("1-000000004", "2023-01-01", 1672531200),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_creating_extrapolated_ratios_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("rolling_average_model", DoubleType(), True),
            StructField("first_submission_time", LongType(), False),
            StructField("last_submission_time", LongType(), False),
            StructField("first_rolling_average", DoubleType(), True),
            StructField("last_rolling_average", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", 1675000000, 1.0, 1678000000, 1680000000, 2.0, 3.0),
        ("1-000000002", 1675000000, 1.0, 1678000000, 1680000000, 1.0, 3.0),
        ("1-000000003", 1675000000, 2.0, 1670000000, 1672000000, 3.0, 1.7),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df


def generate_data_for_creating_extrapolated_model_outputs_df():
    spark = utils.get_spark()

    schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("snapshot_date", StringType(), False),
            StructField("unix_time", LongType(), False),
            StructField("rolling_average_model", DoubleType(), True),
            StructField("first_submission_time", LongType(), False),
            StructField("last_submission_time", LongType(), False),
            StructField("first_job_count", DoubleType(), True),
            StructField("last_job_count", DoubleType(), True),
            StructField("extrapolation_ratio", DoubleType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("1-000000001", "2023-01-01", 1675000000, 1.0, 1678000000, 1680000000, 15.0, 10.0, 0.5),
        ("1-000000002", "2023-02-01", 1675000000, 1.0, 1678000000, 1680000000, 15.0, 10.0, 1.0),
        ("1-000000003", "2023-03-01", 1675000000, 2.0, 1670000000, 1672000000, 10.0, 15.0, 1.46882452),
    ]
    # fmt: on
    df = spark.createDataFrame(rows, schema=schema)

    return df
