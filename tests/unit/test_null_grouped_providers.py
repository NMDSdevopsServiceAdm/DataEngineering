import unittest
import warnings

from tests.test_file_data import (
    NullGroupedProvidersData as Data,
)
from tests.test_file_schemas import (
    NullGroupedProvidersSchema as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers import (
    null_grouped_providers as job,
)


class NullGroupedProvidersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.null_grouped_providers_rows,
            Schemas.null_grouped_providers_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_df = job.null_grouped_providers(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_null_grouped_providers_rows,
            Schemas.null_grouped_providers_schema,
        )

    def test_null_grouped_providers_returns_correct_values(
        self,
    ):
        self.assertEqual(
            self.expected_df.collect(),
            self.returned_df.sort(IndCQC.location_id).collect(),
        )


class NullCareHomeGroupedProvidersTests(NullGroupedProvidersTests):
    def setUp(self) -> None:
        super().setUp()

    def test_null_care_home_grouped_providers_returns_correct_values_when_grouped_provider_exists(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_grouped_provider_exists_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_grouped_provider_exists_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_grouped_provider_does_not_exist(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_grouped_provider_does_not_exist_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_grouped_provider_does_not_exist_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_all_locations_are_non_residential(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_all_locations_are_non_residential_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_all_locations_are_non_residential_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_some_locations_are_non_residential_but_grouped_provider_is_care_home(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_some_locations_are_non_residential_but_grouped_provider_is_care_home_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_some_locations_are_non_residential_but_grouped_provider_is_care_home_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_not_enough_locations(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_not_enough_locations_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_not_enough_locations_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_multiple_locations_in_ascwds(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_multiple_locations_in_ascwds_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_multiple_locations_in_ascwds_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )

    def test_null_care_home_grouped_providers_returns_correct_values_when_no_ascwds_data(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.null_care_home_grouped_providers_where_no_ascwds_data_rows,
            Schemas.null_grouped_providers_schema,
        )

        returned_df = job.null_care_home_grouped_providers(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_care_home_grouped_providers_where_no_ascwds_data_rows,
            Schemas.null_grouped_providers_schema,
        )
        self.assertEqual(
            expected_df.collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )
