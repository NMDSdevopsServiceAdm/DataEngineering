import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import worker_tracking
from utils import utils
from tests.test_file_generator import (
    generate_ascwds_stayer_leaver_workplace_data,
    generate_ascwds_stayer_leaver_worker_data,
    generate_workplace_import_dates,
    generate_worker_import_dates,
)


class Worker_Tracking(unittest.TestCase):

    ASCWDS_WORKPLACE = "tests/test_data/tmp/ascwds_workplace.parquet"
    ASCWDS_WORKER = "tests/test_data/tmp/ascwds_worker.parquet"
    WORKPLACE_IMPORT_DATES = "tests/test_data/tmp/workplace_import_dates.parquet"
    WORKER_IMPORT_DATES = "tests/test_data/tmp/worker_import_dates.parquet"

    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder.appName("test_worker_tracking").getOrCreate()
        generate_ascwds_stayer_leaver_workplace_data(self.ASCWDS_WORKPLACE)
        generate_ascwds_stayer_leaver_worker_data(self.ASCWDS_WORKER)
        generate_workplace_import_dates(self.WORKPLACE_IMPORT_DATES)
        generate_worker_import_dates(self.WORKER_IMPORT_DATES)

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(self.ASCWDS_WORKPLACE)
            shutil.rmtree(self.ASCWDS_WORKER)
            shutil.rmtree(self.WORKPLACE_IMPORT_DATES)
            shutil.rmtree(self.WORKER_IMPORT_DATES)
        except OSError():
            pass  # Ignore dir does not exist

    def test_max_import_date_in_two_datasets(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        max_import_date = worker_tracking.max_import_date_in_two_datasets(
            workplace_dates, worker_dates
        )

        self.assertEqual(max_import_date, "20220101")

    def test_get_start_period_import_date(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        start_period_import_date = worker_tracking.get_start_period_import_date(
            workplace_dates, worker_dates, "20220101"
        )

        self.assertEqual(start_period_import_date, "20201225")

    def test_get_start_and_end_period_import_dates(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        end_period_import_date = worker_tracking.max_import_date_in_two_datasets(
            workplace_dates, worker_dates
        )

        start_period_import_date = worker_tracking.get_start_period_import_date(
            workplace_dates, worker_dates, end_period_import_date
        )

        self.assertEqual(end_period_import_date, "20220101")
        self.assertEqual(start_period_import_date, "20201225")

    def test_filter_workplaces(self):

        filtered_df = worker_tracking.filter_workplaces(
            self.ASCWDS_WORKPLACE, "20210101", "20220101"
        )

        self.assertEqual(filtered_df.columns, ["establishmentid"])

        # asserts equivalent items are present in both sequences
        self.assertCountEqual(
            filtered_df.select("establishmentid").rdd.flatMap(lambda x: x).collect(),
            [
                "108",
                "109",
                "111",
                "112",
            ],
        )

    def test_get_employees_with_new_identifier(self):
        spark = utils.get_spark()
        columns = ["establishmentid", "workerid", "emplstat", "other_data"]
        rows = [
            ("1", "100", "190", "other_data"),
            ("20", "20", "195", "other_data"),
            ("300", "3", "191", "other_data"),
        ]
        worker_df = spark.createDataFrame(rows, columns)

        df = worker_tracking.get_employees_with_new_identifier(worker_df)

        self.assertEqual(
            df.columns,
            [
                "establishmentid",
                "workerid",
                "emplstat",
                "other_data",
                "establishmentid_workerid",
            ],
        )
        self.assertCountEqual(
            df.select("establishmentid_workerid").rdd.flatMap(lambda x: x).collect(),
            [
                "1_100",
                "300_3",
            ],
        )

    def test_determine_stayer_or_leaver(self):
        spark = utils.get_spark()

        workers = spark.read.parquet(self.WORKER_IMPORT_DATES)

        start_df = worker_tracking.determine_stayer_or_leaver(
            workers, "20210101", "20220101"
        )

        self.assertEqual(start_df.count(), 11)

        collected_start_df = start_df.collect()
        self.assertEqual(collected_start_df[0]["stayer_or_leaver"], "still employed")
        self.assertEqual(collected_start_df[1]["stayer_or_leaver"], "leaver")

    def test_main(self):
        output_df = worker_tracking.main(
            self.ASCWDS_WORKPLACE,
            self.ASCWDS_WORKER,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 20)
        self.assertEqual(
            output_df.columns,
            [
                "establishmentid_workerid",
                "establishmentid",
                "workerid",
                "emplstat",
                "import_date",
                "other_col",
                "stayer_or_leaver",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
