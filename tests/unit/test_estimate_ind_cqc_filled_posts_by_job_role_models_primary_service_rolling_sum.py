import unittest

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils.models import (
    primary_service_rolling_sum as job,
)
from tests.test_file_data import EstimateJobRolesPrimaryServiceRollingSumData as Data
from tests.test_file_schemas import (
    EstimateJobRolesPrimaryServiceRollingSumSchemas as Schemas,
)


class EstimateJobRolesPrimaryServiceRollingSumTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class AddRollingSumPartitionedByPrimaryServiceTyoe(
    EstimateJobRolesPrimaryServiceRollingSumTests
):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days_in_rolling_sum = 1

    def test_add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_data,
            Schemas.add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_data,
            Schemas.expected_add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled_schema,
        )

        returned_df = job.add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled(
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
    EstimateJobRolesPrimaryServiceRollingSumTests
):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days_in_rolling_sum = 185
        self.list_of_job_roles_for_tests = Data.list_of_job_roles_for_tests

        self.test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_one_primary_service_present_rows,
            Schemas.primary_service_rolling_sum_schema,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_one_primary_service_present_rows,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        self.returned_df = job.calculate_rolling_sum_of_job_roles(
            self.test_df,
            self.number_of_days_in_rolling_sum,
            self.list_of_job_roles_for_tests,
        )

    def test_primary_service_rolling_sum_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_primary_service_rolling_sum_returns_original_row_count(
        self,
    ):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_primary_service_rolling_sum_returns_expected_rolling_sum_when_one_primary_service_present(
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

    def test_primary_service_rolling_sum_returns_expected_rolling_sum_when_multiple_primary_services_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_multiple_primary_services_present_rows,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_multiple_primary_services_present_rows,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = job.calculate_rolling_sum_of_job_roles(
            test_df,
            self.number_of_days_in_rolling_sum,
            self.list_of_job_roles_for_tests,
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

    def test_primary_service_rolling_sum_does_not_sum_values_outside_of_rolling_window(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.primary_service_rolling_sum_when_days_not_within_rolling_window_rows,
            Schemas.primary_service_rolling_sum_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_sum_when_days_not_within_rolling_window_rows,
            Schemas.expected_primary_service_rolling_sum_schema,
        )

        returned_df = job.calculate_rolling_sum_of_job_roles(
            test_df,
            self.number_of_days_in_rolling_sum,
            self.list_of_job_roles_for_tests,
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
