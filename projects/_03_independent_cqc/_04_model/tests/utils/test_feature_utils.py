import unittest

import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._04_model.utils import feature_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    FeaturesEngineeringUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    FeaturesEngineeringUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class AddArrayColumnCountTests(unittest.TestCase):
    def test_returns_expected_lf_when_one_element_in_array(self):
        test_lf = pl.LazyFrame(
            Data.add_array_column_count_with_one_element_rows,
            Schemas.add_array_column_count_schema,
            orient="row",
        )
        returned_lf = job.add_array_column_count(
            test_lf,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.services_offered,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_add_array_column_count_with_one_element_rows,
            Schemas.expected_add_array_column_count_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_lf_when_multiple_elements_in_array(self):
        test_lf = pl.LazyFrame(
            Data.add_array_column_count_with_multiple_elements_rows,
            Schemas.add_array_column_count_schema,
            orient="row",
        )
        returned_lf = job.add_array_column_count(
            test_lf,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.services_offered,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_add_array_column_count_with_multiple_elements_rows,
            Schemas.expected_add_array_column_count_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_zero_when_array_is_empty(self):
        test_lf = pl.LazyFrame(
            Data.add_array_column_count_with_empty_array_rows,
            Schemas.add_array_column_count_schema,
            orient="row",
        )
        returned_lf = job.add_array_column_count(
            test_lf,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.services_offered,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_add_array_column_count_with_empty_array_rows,
            Schemas.expected_add_array_column_count_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_zero_when_array_is_null(self):
        test_lf = pl.LazyFrame(
            Data.add_array_column_count_with_null_value_rows,
            Schemas.add_array_column_count_schema,
            orient="row",
        )
        returned_lf = job.add_array_column_count(
            test_lf,
            new_col_name=IndCQC.service_count,
            col_to_check=IndCQC.services_offered,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_add_array_column_count_with_null_value_rows,
            Schemas.expected_add_array_column_count_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddDateIndexColumnTests(unittest.TestCase):
    def test_returns_same_index_for_same_import_dates(self):
        test_lf = pl.LazyFrame(
            Data.add_date_index_column_same_index_for_same_date_rows,
            Schemas.add_date_index_column_schema,
            orient="row",
        )
        returned_lf = job.add_date_index_column(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_add_date_index_column_same_index_for_same_date_rows,
            Schemas.expected_add_date_index_column_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_dense_ranked_indexes(self):
        test_lf = pl.LazyFrame(
            Data.add_date_index_column_applies_incremental_index_rows,
            Schemas.add_date_index_column_schema,
            orient="row",
        )
        returned_lf = job.add_date_index_column(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_add_date_index_column_applies_incremental_index_rows,
            Schemas.expected_add_date_index_column_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_indexes_separated_on_care_home_column(self):
        test_lf = pl.LazyFrame(
            Data.add_date_index_column_indexes_by_care_home_rows,
            Schemas.add_date_index_column_schema,
            orient="row",
        )
        returned_lf = job.add_date_index_column(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_add_date_index_column_indexes_by_care_home_rows,
            Schemas.expected_add_date_index_column_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CapIntegerAtMaxValueTests(unittest.TestCase):
    def test_returns_expected_lf(self):
        test_lf = pl.LazyFrame(
            Data.cap_integer_at_max_value_rows,
            Schemas.cap_integer_at_max_value_schema,
            orient="row",
        )
        returned_lf = job.cap_integer_at_max_value(
            test_lf,
            col_name=IndCQC.service_count,
            max_value=2,
            new_col_name=IndCQC.service_count_capped,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_cap_integer_at_max_value_rows,
            Schemas.expected_cap_integer_at_max_value_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class ExpandEncodeAndExtractFeaturesTests(unittest.TestCase):
    def test_returns_expected_lf_when_not_array(self):
        test_lf = pl.LazyFrame(
            Data.expand_encode_and_extract_features_when_not_array_rows,
            Schemas.expand_encode_and_extract_features_when_not_array_schema,
            orient="row",
        )
        returned_lf = job.expand_encode_and_extract_features(
            test_lf,
            Schemas.col_with_categories,
            Data.expand_encode_and_extract_features_lookup_dict,
            is_array_col=False,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_expand_encode_and_extract_features_when_not_array_rows,
            Schemas.expected_expand_encode_and_extract_features_when_not_array_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_lf_when_is_array(self):
        test_lf = pl.LazyFrame(
            Data.expand_encode_and_extract_features_when_is_array_rows,
            Schemas.expand_encode_and_extract_features_when_is_array_schema,
            orient="row",
        )
        returned_lf = job.expand_encode_and_extract_features(
            test_lf,
            Schemas.col_with_categories,
            Data.expand_encode_and_extract_features_lookup_dict,
            is_array_col=True,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_expand_encode_and_extract_features_when_is_array_rows,
            Schemas.expected_expand_encode_and_extract_features_when_is_array_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class GroupRuralUrbanSparseCategoriesTests(unittest.TestCase):
    def test_returns_original_values_when_does_not_contain_sparse(self):
        test_lf = pl.LazyFrame(
            Data.group_rural_urban_sparse_categories_non_sparse_do_not_change_rows,
            Schemas.group_rural_urban_sparse_categories_schema,
            orient="row",
        )
        returned_lf = job.group_rural_urban_sparse_categories(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_group_rural_urban_sparse_categories_non_sparse_do_not_change_rows,
            Schemas.expected_group_rural_urban_sparse_categories_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_value_when_does_contain_sparse(self):
        test_lf = pl.LazyFrame(
            Data.group_rural_urban_sparse_categories_identifies_sparse_rows,
            Schemas.group_rural_urban_sparse_categories_schema,
            orient="row",
        )
        returned_lf = job.group_rural_urban_sparse_categories(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_group_rural_urban_sparse_categories_identifies_sparse_rows,
            Schemas.expected_group_rural_urban_sparse_categories_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_lf_when_combination_of_indicators(self):
        test_lf = pl.LazyFrame(
            Data.group_rural_urban_sparse_categories_combination_rows,
            Schemas.group_rural_urban_sparse_categories_schema,
            orient="row",
        )
        returned_lf = job.group_rural_urban_sparse_categories(test_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_group_rural_urban_sparse_categories_combination_rows,
            Schemas.expected_group_rural_urban_sparse_categories_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AddSquaredColumnTests(unittest.TestCase):
    def test_selection_and_filtering_returns_expected_lazyframe(self):
        test_lf = pl.LazyFrame(
            Data.add_squared_column_rows,
            Schemas.add_squared_column_schema,
            orient="row",
        )

        returned_lf = job.add_squared_column(
            test_lf, IndCQC.cqc_location_import_date_indexed
        )

        expected_lf = pl.LazyFrame(
            Data.expected_add_squared_column_rows,
            Schemas.expected_add_squared_column_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class SelectAndFilterFeaturesDataTests(unittest.TestCase):

    def setUp(self):
        self.dependent = "dependent"
        self.partition_keys = ["import_date"]

        self.test_lf = pl.LazyFrame(
            Data.select_and_filter_features_rows,
            Schemas.select_and_filter_features_schema,
            orient="row",
        )

    def test_selection_and_filtering_returns_expected_lazyframe(self):
        features = ["feature_1", "feature_2", "feature_3"]

        returned_lf = job.select_and_filter_features_data(
            self.test_lf, features, self.dependent, self.partition_keys
        )

        expected_lf = pl.LazyFrame(
            Data.expected_select_and_filter_features_rows,
            Schemas.expected_select_and_filter_features_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_missing_columns_raises_error(self):
        features = ["feature_1", "missing_feature"]

        with self.assertRaises(ValueError) as cm:
            job.select_and_filter_features_data(
                self.test_lf, features, self.dependent, self.partition_keys
            )

        self.assertEqual(
            "Missing columns in LazyFrame: ['missing_feature']", str(cm.exception)
        )
