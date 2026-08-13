import polars as pl
import pytest

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.validation_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidationUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestCareHomeMatchesPrimaryServiceTypeExpr:
    @pytest.mark.parametrize(
        "care_home, primary_service_type, expected",
        [
            case.as_pytest_param()
            for case in Data.care_home_matches_primary_service_type_test_cases
        ],
    )
    def test_function_returns_expected_value(
        self, care_home, primary_service_type, expected
    ):
        df = pl.DataFrame(
            {
                IndCQC.care_home: [care_home],
                IndCQC.primary_service_type: [primary_service_type],
            }
        )

        result = df.select(job.care_home_matches_primary_service_type_expr())

        assert result.item() is expected


class TestSharedLivesServicesOfferedExpr:
    @pytest.mark.parametrize(
        "primary_service_type_second_level, services_offered, expected",
        [
            case.as_pytest_param()
            for case in Data.shared_lives_services_offered_test_cases
        ],
    )
    def test_function_returns_expected_value(
        self, primary_service_type_second_level, services_offered, expected
    ):
        df = pl.DataFrame(
            {
                IndCQC.primary_service_type_second_level: [
                    primary_service_type_second_level
                ],
                IndCQC.services_offered: [services_offered],
            }
        )

        result = df.select(job.shared_lives_services_offered_expr())

        assert result.item() is expected


class TestCareHomeWithNursingServicesOfferedExpr:
    @pytest.mark.parametrize(
        "primary_service_type_second_level, services_offered, expected",
        [
            case.as_pytest_param()
            for case in Data.care_home_with_nursing_services_offered_test_cases
        ],
    )
    def test_function_returns_expected_value(
        self, primary_service_type_second_level, services_offered, expected
    ):
        df = pl.DataFrame(
            {
                IndCQC.primary_service_type_second_level: [
                    primary_service_type_second_level
                ],
                IndCQC.services_offered: [services_offered],
            }
        )

        result = df.select(job.care_home_with_nursing_services_offered_expr())

        assert result.item() is expected


class TestCareHomeWithoutNursingServicesOfferedExpr:
    @pytest.mark.parametrize(
        "primary_service_type_second_level, services_offered, expected",
        [
            case.as_pytest_param()
            for case in Data.care_home_without_nursing_services_offered_test_cases
        ],
    )
    def test_function_returns_expected_value(
        self, primary_service_type_second_level, services_offered, expected
    ):
        df = pl.DataFrame(
            {
                IndCQC.primary_service_type_second_level: [
                    primary_service_type_second_level
                ],
                IndCQC.services_offered: [services_offered],
            }
        )

        result = df.select(job.care_home_without_nursing_services_offered_expr())

        assert result.item() is expected
