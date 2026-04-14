import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRole04EstimateData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRole04EstimateSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate"


manager_roles = Data.test_manager_roles
non_rm_manager_roles = [
    role for role in manager_roles if role != MainJobRoleLabels.registered_manager
]
test_non_rm_manager_condition = pl.col(IndCQC.main_job_role_clean_labelled).is_in(
    non_rm_manager_roles
)


class TestMain(unittest.TestCase):
    TEST_IMPUTED_SOURCE = "some/directory"
    TEST_METADATA_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/other/directory"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.adjust_managerial_roles")
    @patch(f"{PATCH_PATH}.count_cqc_rm")
    @patch(f"{PATCH_PATH}.calculate_estimated_filled_posts_by_job_role")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
        calculate_estimated_filled_posts_by_job_role_mock: Mock,
        count_cqc_rm_mock: Mock,
        adjust_managerial_roles_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.TEST_IMPUTED_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        calculate_estimated_filled_posts_by_job_role_mock.assert_called_once()
        count_cqc_rm_mock.assert_called_once()
        adjust_managerial_roles_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.TEST_DESTINATION,
            partition_cols=[Keys.year],
            append=False,
        )


class TestCalculateEstimatedFilledPostsByJobRole(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            data=Data.calculate_estimated_filled_posts_by_job_role_rows,
            schema=Schemas.calculate_estimated_filled_posts_by_job_role_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(
            [
                IndCQC.ascwds_job_role_ratios_merged,
                IndCQC.estimate_filled_posts_by_job_role,
            ]
        )
        returned_lf = job.calculate_estimated_filled_posts_by_job_role(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CountCqcRmTest(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            data=Data.count_cqc_rm_rows,
            schema=Schemas.count_cqc_rm_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.registered_manager_count)
        returned_lf = test_lf.with_columns(
            job.count_cqc_rm().alias(IndCQC.registered_manager_count)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AdjustManagerialRolesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            data=Data.adjust_managerial_roles_rows,
            schema=Schemas.adjust_managerial_roles_schema,
            orient="row",
        )
        self.expected_lf = pl.LazyFrame(
            data=Data.expected_adjust_managerial_roles_rows,
            schema=Schemas.expected_adjust_managerial_roles_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.calculate_reg_man_difference")
    @patch(f"{PATCH_PATH}.calculate_non_rm_managerial_distribution")
    @patch(f"{PATCH_PATH}.distribute_rm_difference")
    def test_function_has_expected_calls(
        self,
        distribute_rm_difference: Mock,
        calculate_non_rm_managerial_distribution: Mock,
        calculate_reg_man_difference: Mock,
    ):
        job.adjust_managerial_roles(self.test_lf, non_rm_manager_roles)

        distribute_rm_difference.assert_called_once()
        calculate_non_rm_managerial_distribution.assert_called_once()
        calculate_reg_man_difference.assert_called_once()

    def test_function_returns_expected_values(self):
        returned_lf = job.adjust_managerial_roles(
            self.test_lf,
            non_rm_manager_roles,
        )

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)


class TestCalculateRegManDifference:
    @pytest.mark.parametrize(
        "calculate_reg_man_difference_test_data",
        [
            case.as_pytest_param()
            for case in Data.calculate_reg_man_difference_test_cases
        ],
    )
    def test_function_returns_expected_values(
        self, calculate_reg_man_difference_test_data
    ):
        expected_lf = pl.LazyFrame(
            calculate_reg_man_difference_test_data,
            Schemas.expected_calculate_reg_man_difference_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.difference_between_estimate_and_cqc_registered_managers
        )
        returned_lf = job.calculate_reg_man_difference(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCalculateNonRmManagerialDistribution:
    @pytest.mark.parametrize(
        "calculate_non_rm_managerial_distribution_test_data",
        [
            case.as_pytest_param()
            for case in Data.calculate_non_rm_managerial_distribution_test_cases
        ],
    )
    def test_function_returns_expected_values(
        self, calculate_non_rm_managerial_distribution_test_data
    ):
        expected_lf = pl.LazyFrame(
            calculate_non_rm_managerial_distribution_test_data,
            Schemas.expected_calculate_non_rm_managerial_distribution_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
        )
        returned_lf = job.calculate_non_rm_managerial_distribution(
            input_lf,
            test_non_rm_manager_condition,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestDistributeRmDifference:
    @pytest.mark.parametrize(
        "distribute_rm_difference_test_data",
        [case.as_pytest_param() for case in Data.distribute_rm_difference_test_cases],
    )
    def test_function_returns_expected_values(self, distribute_rm_difference_test_data):
        expected_lf = pl.LazyFrame(
            distribute_rm_difference_test_data,
            Schemas.expected_distribute_rm_difference_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted
        )
        returned_lf = job.distribute_rm_difference(
            input_lf,
            test_non_rm_manager_condition,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
