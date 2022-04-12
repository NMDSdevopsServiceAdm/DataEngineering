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


def generate_all_job_roles_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["master_locationid", "primary_service_type", "main_job_role", "estimate_job_role_count_2021"]

    rows = [
        ("1-000000001", "Care home without nursing", "1", 3.5),
        ("1-000000001", "Care home without nursing", "7", 0.5),
        ("1-000000001", "Care home without nursing", "8", 1.0),
        ("1-000000002", "Care home without nursing", "1", 15.0),
        ("1-000000002", "Care home without nursing", "7", 25.0),
        ("1-000000002", "Care home without nursing", "8", 20.0),
        ("1-000000003", "Care home with nursing", "1", 20.0),
        ("1-000000003", "Care home with nursing", "7", 10.0),
        ("1-000000003", "Care home with nursing", "8", 20.0),
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
