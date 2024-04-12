from datetime import date

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
)

from schemas import cqc_care_directory_schema
from utils import utils


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
