from pyspark.sql import SparkSession
import sys
import pyspark
import argparse

worker_fields_required = [
    "period",
    "establishmentid",
    "tribalid",
    "tribalid_worker",
    "parentid",
    "orgid",
    "nmdsid",
    "workerid",
    "wrkglbid",
    "createddate",
    "updateddate",
    "savedate",
    "regtype",
    "providerid",
    "locationid",
    "mainstid",
    "emplstat",
    "mainjrid",
    "gender",
    "gender_changedate",
    "gender_savedate",
    "salaryint",
    "salary",
    "hrlyrate",
    "pay_changedate",
    "pay_savedate",
]
workplace_fields_required = [
    "establishmentid",
    "establishmentname",
    "esttype",
    "postcode",
    "address",
    "totalstaff",
    "totalstarters",
    "totalleavers",
    "totalvacancies",
]

join_on_field = "establishmentid"
join_type = "inner"


def main(worker_source, workplace_source, destination):
    spark = SparkSession.builder.appName(
        "sfc_data_engineering_csv_to_parquet"
    ).getOrCreate()

    worker_df = read_parquet(spark, worker_source)
    workplace_df = read_parquet(spark, workplace_source)

    joined_df = denormalise(worker_df, workplace_df)
    write_parquet(joined_df, destination)


def denormalise(df1, df2):
    joined_df = df1.select(worker_fields_required).join(
        df2.select(workplace_fields_required), join_on_field, join_type
    )

    return joined_df


def write_parquet(df, destination):
    df.write.parquet(destination)


def read_parquet(spark, source):

    df = spark.read.parquet(source)

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--worker_source",
        help="A source parquet dir containing worker related rows",
        required=True,
    )
    parser.add_argument(
        "--workplace_source",
        help="A source parquet dir containing workplace related rows",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return args.worker_source, args.workplace_source, args.destination


if __name__ == "__main__":
    print("Spark job 'format_fields' starting...")
    print(f"Job parameters: {sys.argv}")
    worker_source, workplace_source, destination = collect_arguments()
    main(worker_source, workplace_source, destination)

    print("Spark job 'format_fields' done")
