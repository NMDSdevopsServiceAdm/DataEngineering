from utils import utils
import datetime


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


def generate_cqc_locations_parquet(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationid",
        "providerid",
        "name",
        "postalcode",
        "type",
        "registrationstatus",
        "localauthority",
        "region",
        "random_col",
    ]

    rows = [
        (
            "1-0000000001",
            "1-0000000001",
            "name 1",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 1",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000002",
            "1-0000000002",
            "name 2",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 1",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000003",
            "1-0000000003",
            "name 3",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 1",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000004",
            "1-0000000004",
            "name 4",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 2",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000005",
            "1-0000000005",
            "name 5",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 2",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000006",
            "1-0000000001",
            "name 6",
            "AB1 2CD",
            "Other",
            "Registered",
            "LA 2",
            "Region 1",
            "ignore",
        ),
        (
            "1-0000000007",
            "1-0000000002",
            "name 7",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 3",
            "Region 2",
            "ignore",
        ),
        (
            "1-0000000008",
            "1-0000000003",
            "name 8",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 3",
            "Region 2",
            "ignore",
        ),
        (
            "1-0000000009",
            "1-0000000004",
            "name 9",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 3",
            "Region 2",
            "ignore",
        ),
        (
            "1-0000000010",
            "1-0000000005",
            "name 10",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 4",
            "Region 2",
            "ignore",
        ),
        (
            "1-0000000011",
            "1-0000000001",
            "name 11",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 4",
            "Region 2",
            "ignore",
        ),
        (
            "1-0000000012",
            "1-0000000002",
            "name 12",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 5",
            "Region 3",
            "ignore",
        ),
        (
            "1-0000000013",
            "1-0000000003",
            "name 13",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 5",
            "Region 3",
            "ignore",
        ),
        (
            "1-0000000014",
            "1-0000000004",
            "name 14",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 5",
            "Region 3",
            "ignore",
        ),
        (
            "1-0000000015",
            "1-0000000005",
            "name 15",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 5",
            "Region 3",
            "ignore",
        ),
        (
            "1-0000000016",
            "1-0000000001",
            "name 16",
            "AB1 2CD",
            "Social Care Org",
            "Other",
            "LA 5",
            "Region 3",
            "ignore",
        ),
        (
            "1-0000000017",
            "1-0000000002",
            "name 17",
            "AB1 2CD",
            "Social Care Org",
            "Registered",
            "LA 6",
            "Region 3",
            "ignore",
        ),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_cqc_providers_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["providerid", "name", "random_col"]

    rows = [
        ("1-0000000001", "provider 1", "ignore"),
        ("1-0000000002", "provider 2", "ignore"),
        ("1-0000000003", "provider 3", "ignore"),
        ("1-0000000004", "provider 4", "ignore"),
        ("1-0000000005", "provider 5", "ignore"),
        ("1-0000000005", "provider 5", "ignore"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df


def generate_ascwds_workplace_parquet(output_destination):
    spark = utils.get_spark()
    columns = [
        "locationid",
        "establishmentid",
        "orgid",
        "isparent",
        "import_date",
        "mupddate",
        "lapermission",
    ]

    rows = [
        ("1-0000000002", "1", "1", "1", "20200202", datetime.date(2020, 2, 2), "-1"),
        ("1-0000000004", "2", "1", "0", "20200202", datetime.date(2020, 2, 2), "0"),
        ("1-0000000006", "3", "1", "0", "20200202", datetime.date(2020, 2, 2), "1"),
        ("1-0000000008", "4", "1", "0", "20200202", datetime.date(2020, 2, 2), "-1"),
        ("1-0000000010", "5", "2", "1", "20200202", datetime.date(2020, 2, 2), "0"),
        ("1-0000000012", "6", "2", "0", "20200202", datetime.date(2020, 2, 2), "1"),
        ("1-0000000014", "7", "2", "0", "20200202", datetime.date(2020, 2, 2), "1"),
        ("1-0000000016", "8", "2", "0", "20200202", datetime.date(2020, 2, 2), "0"),
    ]

    df = spark.createDataFrame(rows, columns)

    if output_destination:
        df.coalesce(1).write.mode("overwrite").parquet(output_destination)

    return df
