from utils import utils


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
