import shutil
import unittest
import warnings
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType

from jobs import prepare_workers
from schemas.worker_schema import WORKER_SCHEMA
from tests.test_file_generator import (
    generate_ascwds_worker_file,
    generate_flexible_worker_file_hours_worked,
    generate_flexible_worker_file_hourly_rate,
)
from utils import utils


class PrepareWorkersTests(unittest.TestCase):

    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_workers").getOrCreate()
        self.test_df = generate_ascwds_worker_file(self.TEST_ASCWDS_WORKER_FILE)
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKER_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_main_adds_aggregated_columns(self):
        df = prepare_workers.main(self.TEST_ASCWDS_WORKER_FILE)

        aggregated_cols = [
            "training",
            "job_role",
            "qualifications",
            "hrs_worked",
            "hourly_rate",
        ]
        cols_removed = [
            "tr01latestdate",
            "jr01flag",
            "ql15year3",
            "averagehours",
            "conthrs",
            "salary",
            "salaryint",
            "hrlyrate",
        ]
        columns_kept = [
            "emplstat",
            "zerohours",
        ]
        training_json = json.loads(df.first()["training"])
        extracted_date = training_json["tr01"]["latestdate"]

        for col in aggregated_cols + columns_kept:
            self.assertIn(col, df.columns)
        for col in cols_removed:
            self.assertNotIn(col, df.columns)
        self.assertEqual(len(df.columns), 70)
        self.assertEqual(extracted_date, "2017-06-15")

    def test_clean(self):
        schema = StructType(
            fields=[
                StructField("tr01flag", StringType(), True),
                StructField("tr01latestdate", StringType(), True),
                StructField("tr01count", StringType(), True),
                StructField("tr02flag", StringType(), True),
                StructField("tr02ac", StringType(), True),
                StructField("tr02nac", StringType(), True),
                StructField("tr02dn", StringType(), True),
                StructField("tr02latestdate", StringType(), True),
            ]
        )
        columns = [
            "ql01year2",
            "tr03flag",
            "jr03flag",
            "ql05achq3",
            "emplstat",
            "zerohours",
            "salaryint",
            "averagehours",
            "conthrs",
            "salary",
            "hrlyrate", "tr01count", "tr02ac", "tr02nac", "tr02dn",
            "distwrkk", "dayssick", "previous_pay",
        ]
        rows = [
            ("0", "1", "1", "1", "190", "-1", "252", "26.5", "20.0", "0.53", "2.0", "0", "1", "0", "1", "10.0", "15.3", "13"),
            ("0", "1", None, "1", "191", "0", "250", "26.5", "20.0", "0.53", "2.0",  "0", "1", "0", "1", "10.0", "15.3", "13"),
        ]
        df = self.spark.createDataFrame(rows, columns)
        cleaned_df = prepare_workers.clean(df, columns, schema)
        cleaned_df_list = cleaned_df.collect()

        self.assertEqual(cleaned_df.count(), 2)
        self.assertEqual(cleaned_df_list[0]["ql01year2"], 0)
        self.assertEqual(cleaned_df_list[0]["zerohours"], -1)
        self.assertEqual(cleaned_df_list[0]["salaryint"], 252)
        self.assertEqual(cleaned_df_list[0]["averagehours"], 26.5)
        self.assertEqual(cleaned_df_list[1]["jr03flag"], None)
        self.assertAlmostEqual(cleaned_df_list[1]["salary"], 0.53)
        self.assertEqual(cleaned_df_list[1]["tr02ac"], 1)
        self.assertEqual(cleaned_df_list[1]["tr02dn"], 1)
        self.assertAlmostEqual(cleaned_df_list[1]["dayssick"], 15.30, places=2)

    def test_get_dataset_worker_has_correct_columns(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        column_names = utils.extract_column_from_schema(WORKER_SCHEMA)

        self.assertEqual(worker_df.columns, column_names)

    def test_get_dataset_worker_has_correct_rows_number(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)

        self.assertEqual(worker_df.count(), 10)

    def test_replace_columns_after_aggregation(self):
        training_cols = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)
        pattern = "^tr\d\d[a-z]"
        df = prepare_workers.replace_columns_with_aggregated_column(
            self.test_df,
            "training",
            udf_function=prepare_workers.get_training_into_json,
            pattern=pattern,
        )

        self.assertEqual("training", df.columns[-1])
        for training_col in training_cols:
            self.assertNotIn(training_col, df.columns)

    def test_get_training_into_json(self):
        columns = [
            "tr01flag",
            "tr01latestdate",
            "tr01count",
            "tr01ac",
            "tr01nac",
            "tr01dn",
            "tr02flag",
            "tr02latestdate",
            "tr02count",
            "tr02ac",
            "tr02nac",
            "tr02dn",
        ]
        rows = [
            (1, "2017-06-15", 1, 0, 0, 0, 0, "2018-06-15", 1, 0, 0, 0),
            (0, "2017-05-15", 1, 0, 0, 0, 0, "2019-06-15", 1, 0, 0, 0),
        ]
        df = self.spark.createDataFrame(rows, columns)
        df = prepare_workers.add_aggregated_column(
            df,
            "training",
            columns,
            prepare_workers.get_training_into_json,
            types=["tr01", "tr02"],
        )

        self.assertEqual(df.columns[-1], "training")
        self.assertEqual(
            df.first()["training"],
            '{"tr01": {"latestdate": "2017-06-15", "count": 1, "ac": 0, "nac": 0, "dn": 0}}',
        )
        self.assertEqual(df.first()["tr02flag"], 0)

    def test_get_job_role_into_json(self):
        columns = ["jr01flag", "jr05flag", "jr16cat8"]
        rows = [(1, 1, 0), (0, 0, 0)]
        df = self.spark.createDataFrame(rows, columns)
        df = prepare_workers.add_aggregated_column(
            df,
            "job_role",
            columns,
            prepare_workers.get_job_role_into_json,
            types=columns,
        )

        self.assertEqual(df.columns[-1], "job_role")
        self.assertEqual(
            df.first()["job_role"],
            '["jr01flag", "jr05flag"]',
        )
        self.assertEqual(df.first()["jr16cat8"], 0)

    def test_get_qualification_into_json(self):
        columns = [
            "ql01achq2",
            "ql01year2",
            "ql34achqe",
            "ql34yeare",
            "ql37achq",
            "ql37year",
        ]
        rows = [
            (1, 2010, 0, 2019, 0, 2020),
            (0, 2010, 1, 2019, 0, 2020),
            (0, 2010, 0, 2019, 3, 2020),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = prepare_workers.add_aggregated_column(
            df,
            "qualification",
            columns,
            prepare_workers.get_qualification_into_json,
            types=["ql01achq2", "ql34achqe", "ql37achq"],
        )
        df_list = df.collect()
        self.assertEqual(df.columns[-1], "qualification")
        self.assertEqual(
            df_list[0]["qualification"], '{"ql01achq2": {"count": 1, "year": 2010}}'
        )
        self.assertEqual(
            df_list[1]["qualification"], '{"ql34achqe": {"count": 1, "year": 2019}}'
        )
        self.assertEqual(
            df_list[2]["qualification"], '{"ql37achq": {"count": 3, "year": 2020}}'
        )
        self.assertEqual(df.first()["ql37achq"], 0)

    def test_calculate_hours_worked_returns_avghrs_for_empl_with_zerohours(self):
        emplstat = 191
        zerohours = 1
        averagehours = 26.5
        conthrs = 8.5
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertIn("hrs_worked", df.columns)
        self.assertEqual(df.first()["hrs_worked"], averagehours)

    def test_calculate_hours_worked_returns_conthrs_for_empl_without_zerohours(self):
        emplstat = 190
        zerohours = 0
        averagehours = 26.5
        conthrs = 8.5
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hrs_worked"], conthrs)

    def test_calculate_hours_worked_returns_avghrs_for_other_empl(self):
        emplstat = 196
        zerohours = 0
        averagehours = 2.0
        conthrs = 30.0
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hrs_worked"], averagehours)

    def test_calculate_hours_worked_returns_conthrs_for_empl_none(self):
        emplstat = -1
        zerohours = 0
        averagehours = 23.6
        conthrs = 20.0
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hrs_worked"], conthrs)

    def test_calculate_hours_worked_returns_avghrs_for_empl_conthrs_none(self):
        emplstat = -1
        zerohours = 0
        averagehours = 26.5
        conthrs = -1
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hrs_worked"], averagehours)

    def test_calculate_hours_worked_returns_one_for_empl_avghrs_conthrs_none(self):
        emplstat = -1
        zerohours = 0
        averagehours = -1
        conthrs = -1
        df = generate_flexible_worker_file_hours_worked(
            emplstat, zerohours, averagehours, conthrs
        )
        columns = [
            "emplstat",
            "zerohours",
            "averagehours",
            "conthrs",
        ]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hrs_worked",
            columns=columns,
            udf_function=prepare_workers.calculate_hours_worked,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hrs_worked"], None)

    def test_calculate_hourly_rate_returns_hrlyrate_calculation(self):
        salary = 5200.0
        salaryint = 250
        hrlyrate = 100.5
        hrs_worked = 2.5
        df = generate_flexible_worker_file_hourly_rate(
            salary, salaryint, hrlyrate, hrs_worked
        )
        columns = ["salary", "salaryint", "hrlyrate", "hrs_worked"]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hourly_rate",
            columns=columns,
            udf_function=prepare_workers.calculate_hourly_pay,
            output_type=FloatType(),
        )

        self.assertIn("hrs_worked", df.columns)
        self.assertIn("hourly_rate", df.columns)
        self.assertEqual(df.first()["hourly_rate"], 40.0)

    def test_calculate_hourly_rate_returns_hrlyrate_from_column(self):
        salary = 5200.0
        salaryint = 252
        hrlyrate = 100.5
        hrs_worked = 2.3
        df = generate_flexible_worker_file_hourly_rate(
            salary, salaryint, hrlyrate, hrs_worked
        )
        columns = ["salary", "salaryint", "hrlyrate", "hrs_worked"]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hourly_rate",
            columns=columns,
            udf_function=prepare_workers.calculate_hourly_pay,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hourly_rate"], 100.5)

    def test_calculate_hourly_rate_returns_none_for_other_salaryint(self):
        salary = 5200.0
        salaryint = -1
        hrlyrate = 100.5
        hrs_worked = 2.3
        df = generate_flexible_worker_file_hourly_rate(
            salary, salaryint, hrlyrate, hrs_worked
        )
        columns = ["salary", "salaryint", "hrlyrate", "hrs_worked"]
        df = prepare_workers.add_aggregated_column(
            df,
            col_name="hourly_rate",
            columns=columns,
            udf_function=prepare_workers.calculate_hourly_pay,
            output_type=FloatType(),
        )

        self.assertEqual(df.first()["hourly_rate"], None)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
