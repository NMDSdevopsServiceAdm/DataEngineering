import shutil
import unittest

from pyspark.sql import SparkSession

import jobs.worker_tracking as job
from utils import utils
from tests.test_file_generator import (
    generate_ascwds_stayer_leaver_workplace_data,
    generate_ascwds_stayer_leaver_worker_data,
    generate_workplace_import_dates,
    generate_worker_import_dates,
    generate_filtered_workplaces,
)


class Worker_Tracking(unittest.TestCase):
    ASCWDS_WORKPLACE = "tests/test_data/tmp/ascwds_workplace.parquet"
    ASCWDS_WORKER = "tests/test_data/tmp/ascwds_worker.parquet"
    WORKPLACE_IMPORT_DATES = "tests/test_data/tmp/workplace_import_dates.parquet"
    WORKER_IMPORT_DATES = "tests/test_data/tmp/worker_import_dates.parquet"
    FILTERED_WORKPLACES = "tests/test_data/tmp/filtered_workplaces.parquet"

    @classmethod
    def setUpClass(self):
        self.spark = SparkSession.builder.appName("test_worker_tracking").getOrCreate()
        generate_ascwds_stayer_leaver_workplace_data(self.ASCWDS_WORKPLACE)
        generate_ascwds_stayer_leaver_worker_data(self.ASCWDS_WORKER)
        generate_workplace_import_dates(self.WORKPLACE_IMPORT_DATES)
        generate_worker_import_dates(self.WORKER_IMPORT_DATES)
        generate_filtered_workplaces(self.FILTERED_WORKPLACES)

    @classmethod
    def tearDownClass(self):
        try:
            shutil.rmtree(self.ASCWDS_WORKPLACE)
            shutil.rmtree(self.ASCWDS_WORKER)
            shutil.rmtree(self.WORKPLACE_IMPORT_DATES)
            shutil.rmtree(self.WORKER_IMPORT_DATES)
            shutil.rmtree(self.FILTERED_WORKPLACES)
        except OSError():
            pass  # Ignore dir does not exist

    def test_max_import_date_in_two_datasets(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        max_import_date = job.max_import_date_in_two_datasets(
            workplace_dates, worker_dates
        )

        self.assertEqual(max_import_date, "20220101")

    def test_get_start_period_import_date(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        start_period_import_date = job.get_start_period_import_date(
            workplace_dates, worker_dates, "20220101"
        )

        self.assertEqual(start_period_import_date, "20201225")

    def test_get_start_and_end_period_import_dates(self):
        spark = utils.get_spark()

        workplace_dates = spark.read.parquet(self.WORKPLACE_IMPORT_DATES)
        worker_dates = spark.read.parquet(self.WORKER_IMPORT_DATES)

        end_period_import_date = job.max_import_date_in_two_datasets(
            workplace_dates, worker_dates
        )

        start_period_import_date = job.get_start_period_import_date(
            workplace_dates, worker_dates, end_period_import_date
        )

        self.assertEqual(end_period_import_date, "20220101")
        self.assertEqual(start_period_import_date, "20201225")

    def test_filter_workplaces(self):
        spark = utils.get_spark()

        workplaces = spark.read.parquet(self.ASCWDS_WORKPLACE)

        filtered_df = job.filter_workplaces(workplaces, "20210101", "20220101")

        self.assertEqual(filtered_df.columns, ["establishmentid"])

        # asserts equivalent items are present in both sequences
        self.assertCountEqual(
            filtered_df.select("establishmentid").rdd.flatMap(lambda x: x).collect(),
            [
                "108",
                "109",
                "111",
            ],
        )

    def test_get_employees_with_new_identifier(self):
        spark = utils.get_spark()

        workers = spark.read.parquet(self.ASCWDS_WORKER)
        final_column_list = workers.columns
        final_column_list.append("establishment_worker_id")

        df = job.get_employees_with_new_identifier(workers)

        self.assertEqual(df.columns, final_column_list)
        self.assertEqual(df.count(), 36)

        collected_df = df.collect()
        self.assertEqual(collected_df[0]["establishment_worker_id"], "108_1")
        self.assertEqual(collected_df[9]["establishment_worker_id"], "109_10")

    def test_determine_stayer_or_leaver(self):
        spark = utils.get_spark()

        workers = spark.read.parquet(self.ASCWDS_WORKER)
        filtered_workplaces = spark.read.parquet(self.FILTERED_WORKPLACES)

        start_df = job.determine_stayer_or_leaver(
            filtered_workplaces, workers, "20210101", "20220101"
        )

        self.assertEqual(start_df.count(), 20)

        collected_start_df = start_df.collect()
        self.assertEqual(collected_start_df[0]["stayer_or_leaver"], "still employed")
        self.assertEqual(collected_start_df[1]["stayer_or_leaver"], "leaver")

    def test_main(self):
        output_df = job.main(
            self.ASCWDS_WORKPLACE,
            self.ASCWDS_WORKER,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 20)
        self.assertEqual(
            output_df.columns,
            [
                "establishment_worker_id",
                "establishmentid",
                "workerid",
                "emplstat",
                "start_period_import_date",
                "stayer_or_leaver",
                "year",
                "month",
                "day",
                "end_period_import_date",
            ],
        )

        collected_output_df = output_df.collect()
        self.assertEqual(collected_output_df[0]["stayer_or_leaver"], "still employed")
        self.assertEqual(collected_output_df[1]["stayer_or_leaver"], "leaver")

        self.assertEqual(collected_output_df[0]["year"], "2022")
        self.assertEqual(collected_output_df[0]["month"], "01")
        self.assertEqual(collected_output_df[0]["day"], "01")
        self.assertEqual(collected_output_df[0]["end_period_import_date"], "20220101")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
