import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import worker_tracking
from utils import utils
from tests.test_file_generator import (
    generate_ascwds_stayer_leaver_workplace_data,
    generate_ascwds_stayer_leaver_worker_start_file,
    generate_ascwds_stayer_leaver_worker_end_file,
    generate_workplace_import_dates,
    generate_worker_import_dates,
)


class CQC_Care_Directory_Tests(unittest.TestCase):

    ASCWDS_WORKPLACE = "tests/test_data/tmp/ascwds_workplace.parquet"
    ASCWDS_WORKER = "tests/test_data/tmp/ascwds_worker.parquet"
    WORKPLACE_IMPORT_DATES = "tests/test_data/tmp/workplace_import_dates.parquet"
    WORKER_IMPORT_DATES = "tests/test_data/tmp/worker_import_dates.parquet"

    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder.appName("test_worker_tracking").getOrCreate()
        generate_ascwds_stayer_leaver_workplace_data(self.ASCWDS_WORKPLACE)
        generate_ascwds_stayer_leaver_worker_start_file(self.ASCWDS_WORKER)
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

    def test_updated_within_time_period(self):

        filtered_df = worker_tracking.updated_within_time_period(
            self.START_PERIOD_WORKPLACE_FILE
        )

        self.assertEqual(filtered_df.count(), 4)
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

    def test_workplaces_in_both_dfs(self):
        df_start = self.spark.createDataFrame(
            [("1",), ("2",), ("3",), ("4",), ("5",)], ["establishmentid"]
        )
        df_end = self.spark.createDataFrame(
            [("2",), ("4",), ("6",)], ["establishmentid"]
        )

        df = worker_tracking.workplaces_in_both_dfs(df_start, df_end)

        self.assertEqual(df.count(), 2)
        self.assertEqual(df.columns, ["establishmentid"])

        collected_df = df.collect()
        self.assertEqual(collected_df[0]["establishmentid"], "2")
        self.assertEqual(collected_df[1]["establishmentid"], "4")

    def test_get_ascwds_worker_df(self):
        estab_list_df = self.spark.createDataFrame(
            [("108",), ("110",), ("111",)], ["establishmentid"]
        )

        df = worker_tracking.get_ascwds_worker_df(
            estab_list_df, self.START_PERIOD_WORKER_FILE
        )

        self.assertEqual(df.count(), 12)
        self.assertEqual(
            df.columns,
            [
                "establishmentid",
                "workerid",
                "emplstat",
                "loads",
                "of",
                "other",
                "columns",
                "establishmentid_workerid",
            ],
        )

    def test_determine_stayer_or_leaver(self):
        spark = utils.get_spark()
        columns = ["establishmentid_workerid", "emplstat", "other", "columns"]
        start_rows = [
            ("10_1", "190", "other", "data"),
            ("10_2", "191", "other", "data"),
            ("10_3", "192", "other", "data"),
        ]
        start_df = spark.createDataFrame(start_rows, columns)

        end_rows = [
            ("10_1", "190", "other", "data"),
            ("10_3", "190", "other", "data"),
        ]
        end_df = spark.createDataFrame(end_rows, columns)

        start_df = worker_tracking.determine_stayer_or_leaver(start_df, end_df)

        self.assertEqual(start_df.count(), 2)

        collected_start_df = start_df.collect()
        self.assertEqual(collected_start_df[0]["stayer_or_leaver"], "still employed")
        self.assertEqual(collected_start_df[1]["stayer_or_leaver"], "leaver")

    def test_main(self):
        output_df = worker_tracking.main(
            self.WORKPLACE_DATA,
            self.WORKER_DATA,
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
                "loads",
                "of",
                "other",
                "columns",
                "stayer_or_leaver",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
