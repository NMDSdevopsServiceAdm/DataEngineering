import os
import shutil
import unittest

from utils import utils
from pyspark.sql import SparkSession
from datetime import date

from environment import environment
from jobs import stayer_vs_leaver_dataset

from tests.test_file_generator import (
    generate_ascwds_stayer_leaver_workplace_start_file,
    generate_ascwds_stayer_leaver_workplace_end_file,
    generate_ascwds_stayer_leaver_worker_start_file,
    generate_ascwds_stayer_leaver_worker_end_file,
)


class CQC_Care_Directory_Tests(unittest.TestCase):

    START_PERIOD_WORKPLACE_FILE = "tests/test_data/tmp/ascwds_workplace_file_start.parquet"
    START_PERIOD_WORKER_FILE = "tests/test_data/tmp/ascwds_worker_file_start.parquet"
    END_PERIOD_WORKPLACE_FILE = "tests/test_data/tmp/ascwds_workplace_file_end.parquet"
    END_PERIOD_WORKER_FILE = "tests/test_data/tmp/ascwds_worker_file_end.parquet"

    @classmethod
    def setUpClass(self):
        os.environ[environment.OS_ENVIRONEMNT_VARIABLE] = environment.DEVELOPMENT

        self.spark = SparkSession.builder.appName("test_stayer_vs_leaver_dataset").getOrCreate()
        generate_ascwds_stayer_leaver_workplace_start_file(self.START_PERIOD_WORKPLACE_FILE)
        generate_ascwds_stayer_leaver_worker_start_file(self.START_PERIOD_WORKER_FILE)
        generate_ascwds_stayer_leaver_workplace_end_file(self.END_PERIOD_WORKPLACE_FILE)
        generate_ascwds_stayer_leaver_worker_end_file(self.END_PERIOD_WORKER_FILE)

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(self.START_PERIOD_WORKPLACE_FILE)
            shutil.rmtree(self.START_PERIOD_WORKER_FILE)
            shutil.rmtree(self.END_PERIOD_WORKPLACE_FILE)
            shutil.rmtree(self.END_PERIOD_WORKER_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_updated_within_time_period(self):

        filtered_df = stayer_vs_leaver_dataset.updated_within_time_period(self.START_PERIOD_WORKPLACE_FILE)

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
        df_start = self.spark.createDataFrame([("1",), ("2",), ("3",), ("4",), ("5",)], ["establishmentid"])
        df_end = self.spark.createDataFrame([("2",), ("4",), ("6",)], ["establishmentid"])

        df = stayer_vs_leaver_dataset.workplaces_in_both_dfs(df_start, df_end)

        self.assertEqual(df.count(), 2)
        self.assertEqual(df.columns, ["establishmentid"])

        collected_df = df.collect()
        self.assertEqual(collected_df[0]["establishmentid"], "2")
        self.assertEqual(collected_df[1]["establishmentid"], "4")

    def test_get_ascwds_workplace_df(self):
        estab_list_df = self.spark.createDataFrame([("108",), ("110",), ("111",)], ["establishmentid"])

        df = stayer_vs_leaver_dataset.get_ascwds_workplace_df(estab_list_df, self.START_PERIOD_WORKER_FILE)

        self.assertEqual(df.count(), 11)
        self.assertEqual(
            df.columns,
            ["establishmentid", "workerid", "mainjrid", "loads", "of", "other", "columns", "establishmentid_workerid"],
        )

    def test_determine_stayer_or_leaver(self):
        spark = utils.get_spark()
        columns = ["establishmentid_workerid", "other", "columns"]
        start_rows = [
            ("10_1", "other", "data"),
            ("10_2", "other", "data"),
        ]
        start_df = spark.createDataFrame(start_rows, columns)

        end_rows = [
            ("10_1", "other", "data"),
            ("10_3", "other", "data"),
        ]
        end_df = spark.createDataFrame(end_rows, columns)

        start_df = stayer_vs_leaver_dataset.determine_stayer_or_leaver(start_df, end_df)

        self.assertEqual(start_df.count(), 2)

        collected_start_df = start_df.collect()
        self.assertEqual(collected_start_df[0]["stayer_or_leaver"], "stayer")
        self.assertEqual(collected_start_df[1]["stayer_or_leaver"], "leaver")

    def test_main(self):
        output_df = stayer_vs_leaver_dataset.main(
            self.START_PERIOD_WORKPLACE_FILE,
            self.START_PERIOD_WORKER_FILE,
            self.END_PERIOD_WORKPLACE_FILE,
            self.END_PERIOD_WORKER_FILE,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 20)
        self.assertEqual(
            output_df.columns,
            [
                "establishmentid_workerid",
                "establishmentid",
                "workerid",
                "mainjrid",
                "loads",
                "of",
                "other",
                "columns",
                "stayer_or_leaver",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
