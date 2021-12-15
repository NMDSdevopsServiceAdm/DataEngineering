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
            .appName("sfc_data_engineering") \
            .getOrCreate()

        return spark


get_spark = SetupSpark()


def generate_s3_dir_date_path(domain, dataset, date):
    dir_prepend = "s3://sfc-data-engineering"
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{dir_prepend}/domain={domain}/dataset={dataset}/year={year}/month={month}/day={day}/import_date={import_date}"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


def write_to_parquet(df, output_dir, append):

    if append:
        df.write.mode('append').parquet(output_dir)
    else:
        df.write.output_dir(output_dir)
