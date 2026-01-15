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


class AddDateIndexColumnTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.add_date_index_column_rows, Schemas.add_date_index_column_schema
        )
        self.returned_df = job.add_date_index_column(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_date_index_column_rows,
            Schemas.expected_add_date_index_column_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_add_date_index_column_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_add_date_index_column_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)


class GroupRuralUrbanSparseCategoriesTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.group_rural_urban_sparse_categories_rows,
            Schemas.group_rural_urban_sparse_categories_schema,
        )

        self.returned_df = job.group_rural_urban_sparse_categories(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_group_rural_urban_sparse_categories_rows,
            Schemas.expected_group_rural_urban_sparse_categories_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_group_rural_urban_sparse_categories_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_group_rural_urban_sparse_categories_returns_expected_data(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][
                    IndCQC.current_rural_urban_indicator_2011_for_non_res_model
                ],
                self.expected_data[i][
                    IndCQC.current_rural_urban_indicator_2011_for_non_res_model
                ],
                f"Returned value in row {i} does not match expected",
            )


class FilterWithoutDormancyFeaturesToPre2025Tests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.filter_without_dormancy_features_to_pre_2025_rows,
            Schemas.filter_without_dormancy_features_to_pre_2025_schema,
        )

        self.returned_df = job.filter_without_dormancy_features_to_pre_2025(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_filter_without_dormancy_features_to_pre_2025_rows,
            Schemas.filter_without_dormancy_features_to_pre_2025_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_cap_integer_at_max_value_returns_original_columns(self):
        self.assertEqual(self.returned_df.columns, self.test_df.columns)

    def test_cap_integer_at_max_value_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)


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
