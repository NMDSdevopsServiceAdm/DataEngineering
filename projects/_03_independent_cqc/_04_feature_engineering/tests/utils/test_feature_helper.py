import unittest
import warnings

from utils.utils import get_spark

from projects._03_independent_cqc._04_feature_engineering.utils import helper as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelFeatures as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelFeatures as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class LocationsFeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()

        warnings.simplefilter("ignore", ResourceWarning)


class ExpandEncodeAndExtractFeaturesTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        col_with_categories: str = "categories"

        not_array_test_df = self.spark.createDataFrame(
            Data.expand_encode_and_extract_features_when_not_array_rows,
            Schemas.expand_encode_and_extract_features_when_not_array_schema,
        )
        (
            self.returned_not_array_df,
            self.returned_array_keys,
        ) = job.expand_encode_and_extract_features(
            not_array_test_df,
            col_with_categories,
            Data.expand_encode_and_extract_features_lookup_dict,
            is_array_col=False,
        )
        self.expected_not_array_df = self.spark.createDataFrame(
            Data.expected_expand_encode_and_extract_features_when_not_array_rows,
            Schemas.expected_expand_encode_and_extract_features_when_not_array_schema,
        )

        is_array_test_df = self.spark.createDataFrame(
            Data.expand_encode_and_extract_features_when_is_array_rows,
            Schemas.expand_encode_and_extract_features_when_is_array_schema,
        )
        (
            self.returned_is_array_df,
            self.returned_is_array_keys,
        ) = job.expand_encode_and_extract_features(
            is_array_test_df,
            col_with_categories,
            Data.expand_encode_and_extract_features_lookup_dict,
            is_array_col=True,
        )
        self.expected_is_array_df = self.spark.createDataFrame(
            Data.expected_expand_encode_and_extract_features_when_is_array_rows,
            Schemas.expected_expand_encode_and_extract_features_when_is_array_schema,
        )

    def test_expand_encode_and_extract_features_returns_expected_key_list(
        self,
    ):
        self.assertEqual(
            self.returned_array_keys,
            Data.expected_expand_encode_and_extract_features_feature_list,
        )

    def test_expand_encode_and_extract_features_returns_expected_columns_when_not_array(
        self,
    ):
        self.assertEqual(
            self.returned_not_array_df.columns, self.expected_not_array_df.columns
        )

    def test_expand_encode_and_extract_features_returns_expected_data_when_not_array(
        self,
    ):
        self.assertEqual(
            self.returned_not_array_df.sort(IndCQC.location_id).collect(),
            self.expected_not_array_df.collect(),
        )

    def test_expand_encode_and_extract_features_returns_expected_columns_when_is_array(
        self,
    ):
        self.assertEqual(
            self.returned_is_array_df.columns, self.expected_is_array_df.columns
        )

    def test_expand_encode_and_extract_features_returns_expected_data_when_is_array(
        self,
    ):
        self.assertEqual(
            self.returned_is_array_df.sort(IndCQC.location_id).collect(),
            self.expected_is_array_df.collect(),
        )


class AddArrayColumnCountTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_one_element_rows,
            Schemas.add_array_column_count_schema,
        )
        self.returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )

    def test_add_array_column_count_adds_new_column(self):
        self.assertTrue(IndCQC.service_count in self.returned_df.columns)

    def test_add_array_column_count_returns_expected_data_when_one_element_in_array(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_one_element_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(self.returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_expected_data_when_multiple_elements_in_array(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_multiple_elements_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_multiple_elements_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_zero_when_array_is_empty(self):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_empty_array_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_empty_array_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_add_array_column_count_returns_zero_when_array_is_null(self):
        test_df = self.spark.createDataFrame(
            Data.add_array_column_count_with_null_value_rows,
            Schemas.add_array_column_count_schema,
        )
        returned_df = job.add_array_column_count(
            df=test_df,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.gac_service_types,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_add_array_column_count_with_null_value_rows,
            Schemas.expected_add_array_column_count_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class VectoriseDataframeTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

    def test_vectorise_dataframe(self):
        list_for_vectorisation = ["col_1", "col_2", "col_3"]

        df = self.spark.createDataFrame(
            Data.vectorise_input_rows, Schemas.vectorise_schema
        )

        output_df = job.vectorise_dataframe(
            df=df, list_for_vectorisation=list_for_vectorisation
        )
        output_data = (
            output_df.sort(IndCQC.location_id).select(IndCQC.features).collect()
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_vectorised_feature_rows,
            Schemas.expected_vectorised_feature_schema,
        )
        expected_data = (
            expected_df.sort(IndCQC.location_id).select(IndCQC.features).collect()
        )

        self.assertEqual(output_data, expected_data)


class CapIntegerAtMaxValueTests(LocationsFeatureEngineeringTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.cap_integer_at_max_value_rows, Schemas.cap_integer_at_max_value_schema
        )

        self.returned_df = job.cap_integer_at_max_value(
            df=test_df,
            col_name=IndCQC.service_count,
            max_value=2,
            new_col_name=IndCQC.service_count_capped,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_cap_integer_at_max_value_rows,
            Schemas.expected_cap_integer_at_max_value_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_cap_integer_at_max_value_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_cap_integer_at_max_value_returns_expected_data(self):
        self.assertEqual(self.returned_data, self.expected_data)


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
