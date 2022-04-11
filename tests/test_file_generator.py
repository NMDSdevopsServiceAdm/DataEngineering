from utils import utils


def generate_ethnicity_parquet(output_destination):
    spark = utils.get_spark()
    columns = ["locationid", "mainjrid", "ethnicity", "import_date"]

    rows = [
        ("1-000000001", "1", "31", "20200301"),
        ("1-000000001", "1", "32", "20200301"),
        ("1-000000001", "1", "33", "20200301"),
        ("1-000000001", "2", "35", "20200301"),
        ("1-000000001", "3", "39", "20200301"),
        ("1-000000001", "3", "99", "20200301"),
        ("1-000000001", "3", "-1", "20200301"),
        ("1-000000002", "1", "34", "20200301"),
        ("1-000000002", "3", "31", "20200301"),
        ("1-000000002", "2", "32", "20200301"),
        ("1-000000003", "1", "33", "20200301"),
        ("1-000000003", "3", "34", "20200301"),
        ("1-000000003", "3", "98", "20200301"),
        ("1-000000004", "1", "39", "20200301"),
        ("1-000000004", "3", "46", "20200301"),
        ("1-000000004", "3", "37", "20200301"),
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
        ("1-000000001", "Care home without nursing", "2", 0.5),
        ("1-000000001", "Care home without nursing", "3", 1.0),
        ("1-000000002", "Care home without nursing", "1", 15.0),
        ("1-000000002", "Care home without nursing", "2", 25.0),
        ("1-000000002", "Care home without nursing", "3", 20.0),
        ("1-000000003", "Care home with nursing", "1", 20.0),
        ("1-000000003", "Care home with nursing", "2", 10.0),
        ("1-000000003", "Care home with nursing", "3", 20.0),
        ("1-000000004", "Care home with nursing", "1", 10.0),
        ("1-000000004", "Care home with nursing", "2", 0.0),
        ("1-000000004", "Care home with nursing", "3", 10.0),
        ("1-000000005", "Care home with nursing", "1", 11.0),
        ("1-000000005", "Care home with nursing", "2", 22.0),
        ("1-000000005", "Care home with nursing", "3", 33.0),
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
