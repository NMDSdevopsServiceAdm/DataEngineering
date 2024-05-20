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
