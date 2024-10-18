import unittest
import warnings

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

    def test_main_function_completes(self):
        pass


class CleanAscwdsAndPirOutliersTests(CleanAscwdsAndPirOutliersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_null_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_greater_than_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_when_pir_greater_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ascwds_and_pir_when_pir_greater_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        returned_df = job.null_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_null_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_less_than_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_when_pir_less_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ascwds_and_pir_when_pir_less_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        returned_df = job.null_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_null_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_pir_data_is_equal_to_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_when_pir_equal_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ascwds_and_pir_when_pir_equal_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        returned_df = job.null_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_null_rows_where_ascwds_less_than_people_directly_employed_nulls_rows_when_missing_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.clean_ascwds_and_pir_when_missing_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_clean_ascwds_and_pir_when_missing_rows,
            Schemas.clean_ascwds_and_pir_schema,
        )
        returned_df = job.null_rows_where_ascwds_less_than_people_directly_employed(
            test_df
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
