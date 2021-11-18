import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys


def main():
    args = getResolvedOptions(
        sys.argv, ["SOURCE_CSV", "DESTINATION_DIR"])

    spark = SparkSession.builder \
        .appName("sfc_data_engineering_csv_to_parquet") \
        .getOrCreate()

    df = spark.read.csv(args["SOURCE_CSV"], header=True)

    df.write.parquet(args["DESTINATION_DIR"])


if __name__ == '__main__':
    print("Spark job 'csv_to_parquet' starting...")
    main()
    print("Spark job 'csv_to_parquet' done")
