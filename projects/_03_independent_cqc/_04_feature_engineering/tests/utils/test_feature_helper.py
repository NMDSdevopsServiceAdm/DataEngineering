import unittest
import warnings

from projects._03_independent_cqc._04_feature_engineering.utils import helper as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelFeatures as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelFeatures as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.utils import get_spark


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)


class AddSquaredColumnTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.add_squared_column_rows,
            Schemas.add_squared_column_schema,
        )

        self.returned_df = job.add_squared_column(
            test_df, IndCQC.cqc_location_import_date_indexed
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_squared_column_rows,
            Schemas.expected_add_squared_column_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_add_squared_column_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_add_squared_column_returns_expected_data(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCQC.cqc_location_import_date_indexed_squared],
                self.expected_data[i][IndCQC.cqc_location_import_date_indexed_squared],
                3,
                f"Returned value in row {i} does not match expected",
            )
