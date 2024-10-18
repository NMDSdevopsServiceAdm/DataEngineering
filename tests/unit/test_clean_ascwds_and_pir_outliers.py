import unittest
import warnings
from unittest.mock import Mock, patch

from pyspark.sql import DataFrame

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from tests.test_file_data import CleanAscwdsAndPirOutliersData as Data
from tests.test_file_schemas import CleanAscwdsAndPirOutliersSchemas as Schemas
from utils import utils
import utils.ind_cqc_filled_posts_utils.clean_ascwds_and_pir_outliers as job


class CleanAscwdsAndPirOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(CleanAscwdsAndPirOutliersTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_rows, Schemas.clean_ascwds_and_pir_schema
        )

    @patch(
        "utils.ind_cqc_filled_posts_utils.clean_ascwds_and_pir_outliers.clean_ascwds_and_pir_outliers"
    )
    def test_main_calls_all_functions(
        self,
        clean_ascwds_and_pir_outliers_mock: Mock,
    ):
        returned_df = job.clean_ascwds_and_pir_outliers(self.test_df)
        clean_ascwds_and_pir_outliers_mock.assert_called_once()

    def test_main_returns_dataframe(
        self,
    ):
        returned_df = job.clean_ascwds_and_pir_outliers(self.test_df)
        self.assertIsInstance(returned_df, DataFrame)

    def test_main_returns_unchanged_row_count(
        self,
    ):
        returned_df = job.clean_ascwds_and_pir_outliers(self.test_df)
        self.assertEqual(self.test_df.count(), returned_df.count())


class NullRowsWhereAscwdsLessThanPeopleDirectlyEmployedTests(
    CleanAscwdsAndPirOutliersTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_check_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_greater_than_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.people_directly_employed_greater_than_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_people_directly_employed_greater_than_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        returned_df = job.check_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_check_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_less_than_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.people_directly_employed_less_than_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_people_directly_employed_less_than_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        returned_df = job.check_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_check_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_equal_to_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.people_directly_employed_equals_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_people_directly_employed_equals_ascwds_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        returned_df = job.check_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_check_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_missing_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.missing_data_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_missing_data_rows,
            Schemas.check_rows_where_ascwds_less_than_people_directly_employed_schema,
        )
        returned_df = job.check_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
