import os
import shutil
import unittest

from pyspark.sql import SparkSession
from datetime import date

from environment import environment
from jobs import stayer_vs_leaver_dataset

from tests.test_file_generator import (
    generate_ascwds_stayer_leaver_workplace_start_file,
    generate_ascwds_stayer_leaver_workplace_end_file,
    generate_ascwds_stayer_leaver_worker_start_file,
)


class CQC_Care_Directory_Tests(unittest.TestCase):

    START_PERIOD_WORKPLACE_FILE = "tests/test_data/tmp/ascwds_workplace_file_start.parquet"
    START_PERIOD_WORKER_FILE = "tests/test_data/tmp/ascwds_worker_file_start.parquet"
    END_PERIOD_WORKPLACE_FILE = "tests/test_data/tmp/ascwds_workplace_file_end.parquet"

    @classmethod
    def setUpClass(self):
        os.environ[environment.OS_ENVIRONEMNT_VARIABLE] = environment.DEVELOPMENT

        self.spark = SparkSession.builder.appName("test_stayer_vs_leaver_dataset").getOrCreate()
        generate_ascwds_stayer_leaver_workplace_start_file(self.START_PERIOD_WORKPLACE_FILE)
        generate_ascwds_stayer_leaver_worker_start_file(self.START_PERIOD_WORKER_FILE)
        generate_ascwds_stayer_leaver_workplace_end_file(self.END_PERIOD_WORKPLACE_FILE)

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(self.START_PERIOD_WORKPLACE_FILE)
            shutil.rmtree(self.START_PERIOD_WORKER_FILE)
            shutil.rmtree(self.END_PERIOD_WORKPLACE_FILE)
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
        df = stayer_vs_leaver_dataset.get_ascwds_workplace_df(self.START_PERIOD_WORKER_FILE)

        self.assertEqual(df.count(), 21)
        self.assertEqual(df.columns, ["establishmentid", "mainjrid", "loads", "of", "other", "columns"])

    def test_main(self):
        output_df = stayer_vs_leaver_dataset.main(
            self.START_PERIOD_WORKPLACE_FILE,
            self.START_PERIOD_WORKER_FILE,
            self.END_PERIOD_WORKPLACE_FILE,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 3)
        self.assertEqual(
            output_df.columns,
            [
                "establishmentid",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
