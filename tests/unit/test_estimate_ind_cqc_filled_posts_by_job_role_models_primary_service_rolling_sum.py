import unittest

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils.models import (
    primary_service_rolling_sum,
)
from tests.test_file_data import EstimateIndCQCFilledPostsByJobRoleUtilsData as Data
from tests.test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleUtilsSchemas as Schemas,
)


class EstimateIndCQCFilledPostsByJobRoleUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class AddRollingSumPartitionedByPrimaryServiceTyoe(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days_in_rolling_sum = 1

    def test_add_rolling_sum_partitioned_by_primary_service_type(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.add_rolling_sum_partitioned_by_primary_service_type_data,
            Schemas.add_rolling_sum_partitioned_by_primary_service_type_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_add_rolling_sum_partitioned_by_primary_service_type_data,
            Schemas.expected_add_rolling_sum_partitioned_by_primary_service_type_schema,
        )

        returned_df = primary_service_rolling_sum.add_rolling_sum_partitioned_by_primary_service_type(
            test_df,
            self.number_of_days_in_rolling_sum,
            IndCQC.ascwds_job_role_counts_exploded,
            IndCQC.ascwds_job_role_counts_rolling_sum,
        )

        self.assertEqual(
            expected_df.sort(IndCQC.primary_service_type).collect(),
            returned_df.sort(IndCQC.primary_service_type).collect(),
        )


class CalculateRollingSumOfCountOfJobRoles(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days_in_rolling_sum = 185

        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        self.returned_df = (
            primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
                self.test_df, self.number_of_days_in_rolling_sum
            )
        )

    def test_primary_service_rolling_sum_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_primary_service_rolling_sum_returns_same_row_count_as_original_estimated_filled_posts_df(
        self,
    ):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window(
        self,
    ):
        self.assertEqual(
            self.expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            self.returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window_and_null_job_role_count_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window_and_null_job_role_count_present_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_and_within_rolling_window_and_null_job_role_count_present_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
            test_df, self.number_of_days_in_rolling_sum
        )

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_but_not_within_rolling_window(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_but_not_within_rolling_window_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_same_primary_service_type_and_same_location_id_but_not_within_rolling_window_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
            test_df, self.number_of_days_in_rolling_sum
        )

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_primary_service_rolling_sum_when_different_primary_service_types_and_same_location_id_and_within_rolling_window(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_different_primary_service_types_and_same_location_id_and_within_rolling_window_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_different_primary_service_types_and_same_location_id_and_within_rolling_window_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
            test_df, self.number_of_days_in_rolling_sum
        )

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_primary_service_rolling_sum_when_same_primary_service_type_but_different_location_id_and_within_rolling_window(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_same_primary_service_type_but_different_location_id_and_within_rolling_window_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_same_primary_service_type_but_different_location_id_and_within_rolling_window_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
            test_df, self.number_of_days_in_rolling_sum
        )

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_primary_service_rolling_sum_when_same_primary_service_type_and_unix_time_but_different_location_id_within_rolling_window(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_same_primary_service_type_and_unix_time_but_different_location_id_within_rolling_window_data,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_same_primary_service_type_and_unix_time_but_different_location_id_within_rolling_window_data,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = primary_service_rolling_sum.calculate_rolling_sum_of_job_roles(
            test_df, self.number_of_days_in_rolling_sum
        )

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts,
                IndCQC.ascwds_job_role_counts_rolling_sum,
            )
            .orderBy(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )
