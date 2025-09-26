import unittest
import warnings
from unittest.mock import Mock, patch

from pyspark.sql import DataFrame

import projects._03_independent_cqc._02_clean.utils.clean_ascwds_filled_post_outliers.null_grouped_providers as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    NullGroupedProvidersData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    NullGroupedProvidersSchema as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.utils.clean_ascwds_filled_post_outliers.null_grouped_providers"
)


class NullGroupedProvidersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class NullGroupedProvidersConfigTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_minimum_size_of_care_home_location_to_identify(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY,
            25.0,
        )

    def test_minimum_size_of_non_res_location_to_identify(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY,
            50.0,
        )

    def test_posts_per_bed_at_location_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_BED_AT_LOCATION_MULTIPLIER, 4
        )

    def test_posts_per_bed_at_provider_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_BED_AT_PROVIDER_MULTIPLIER, 3
        )

    def test_posts_per_pir_posts_at_location_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_PIR_LOCATION_THRESHOLD, 2.5
        )

    def test_posts_per_pir_posts_at_provider_multiplier(self):
        self.assertEqual(
            job.NullGroupedProvidersConfig.POSTS_PER_PIR_PROVIDER_THRESHOLD, 1.5
        )


class MainTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.null_grouped_providers_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.returned_df = job.null_grouped_providers(self.test_df)

    def test_null_grouped_providers_runs(self):
        self.assertIsInstance(self.returned_df, DataFrame)

    def test_null_grouped_providers_returns_same_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_null_grouped_providers_returns_same_columns(self):
        self.assertEqual(self.returned_df.schema, self.test_df.schema)

    @patch(f"{PATCH_PATH}.null_non_residential_grouped_providers")
    @patch(f"{PATCH_PATH}.null_care_home_grouped_providers")
    @patch(f"{PATCH_PATH}.identify_potential_grouped_providers")
    @patch(f"{PATCH_PATH}.calculate_data_for_grouped_provider_identification")
    def test_null_grouped_providers_calls_functions(
        self,
        calculate_data_for_grouped_provider_identification_mock: Mock,
        identify_potential_grouped_providers_mock: Mock,
        null_care_home_grouped_providers_mock: Mock,
        null_non_residential_grouped_providers_mock: Mock,
    ):
        job.null_grouped_providers(self.test_df)

        calculate_data_for_grouped_provider_identification_mock.assert_called_once_with(
            self.test_df
        )
        identify_potential_grouped_providers_mock.assert_called_once()
        null_care_home_grouped_providers_mock.assert_called_once()
        null_non_residential_grouped_providers_mock.assert_called_once()


class CalculateDataForGroupedProviderIdentificationTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_data_for_grouped_provider_identification_returns_correct_rows_where_provider_has_one_location(
        self,
    ):
        single_location_at_provider_test_df = self.spark.createDataFrame(
            Data.calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows,
            Schemas.calculate_data_for_grouped_provider_identification_schema,
        )
        returned_df = job.calculate_data_for_grouped_provider_identification(
            single_location_at_provider_test_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_data_for_grouped_provider_identification_where_provider_has_one_location_rows,
            Schemas.expected_calculate_data_for_grouped_provider_identification_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )

    def test_calculate_data_for_grouped_provider_identification_returns_correct_rows_where_provider_has_multiple_location(
        self,
    ):
        multiple_location_at_provider_test_df = self.spark.createDataFrame(
            Data.calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows,
            Schemas.calculate_data_for_grouped_provider_identification_schema,
        )
        returned_df = job.calculate_data_for_grouped_provider_identification(
            multiple_location_at_provider_test_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_data_for_grouped_provider_identification_where_provider_has_multiple_location_rows,
            Schemas.expected_calculate_data_for_grouped_provider_identification_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(
                IndCQC.location_id, IndCQC.cqc_location_import_date
            ).collect(),
        )


class IdentifyPotentialGroupedProviderTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_identify_potential_grouped_providers_returns_correct_rows(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.identify_potential_grouped_providers_rows,
            Schemas.identify_potential_grouped_providers_schema,
        )
        returned_df = job.identify_potential_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_identify_potential_grouped_providers_rows,
            Schemas.expected_identify_potential_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )


class NullCareHomeGroupedProvidersTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_null_care_home_grouped_providers_returns_null_when_criteria_met(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_when_meets_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_when_meets_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_original_data_when_criteria_not_met(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_location_does_not_meet_criteria_rows,
            Schemas.null_care_home_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        test_data = test_df.collect()

        for i in range(returned_df.count()):
            self.assertEqual(
                returned_data[i],
                test_data[i],
                f"Row {i} does not match: {returned_data[i]} != {test_data[i]}",
            )


class NullNonResidentialGroupedProvidersTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_null_non_residential_grouped_providers_returns_null_when_criteria_met(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_non_res_grouped_providers_when_meets_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
        )

        returned_df = job.null_non_residential_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_non_res_grouped_providers_when_meets_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_non_residential_grouped_providers_returns_original_data_when_criteria_not_met(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_non_res_grouped_providers_when_does_not_meet_criteria_rows,
            Schemas.null_non_res_grouped_providers_schema,
        )

        returned_df = job.null_non_residential_grouped_providers(test_df)

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        test_data = test_df.collect()

        for i in range(returned_df.count()):
            self.assertEqual(
                returned_data[i],
                test_data[i],
                f"Row {i} does not match: {returned_data[i]} != {test_data[i]}",
            )
