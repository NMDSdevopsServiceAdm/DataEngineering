from pyspark.sql import SparkSession


class SetupSpark(object):
    def __init__(self):
        self.spark = None

    def __call__(self):
        if self.spark:
            return self.spark

        self.spark = self.setupSpark()
        return self.spark

    def setupSpark(self):
        spark = SparkSession.builder \
            .appName("sfc_data_engineering_pull_cqc_locations") \
            .getOrCreate()

        return spark


get_spark = SetupSpark()


def generate_s3_dir_date_path(date):
    pass


def write_to_parquet(df, append):

    if append:
        df.write.mode('append').parquet(OUTPUT_DIR)
    else:
        df.write.parquet(OUTPUT_DIR)
