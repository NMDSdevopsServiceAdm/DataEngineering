from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ImputeJobRoleData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ImputeJobRoleSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType


class TestCreateImputedASCWDSJobRoleCounts:
    @pytest.mark.parametrize(
        "create_imputed_ascwds_job_role_counts_data",
        [
            case.as_pytest_param()
            for case in Data.create_imputed_ascwds_job_role_counts_test_cases
        ],
    )
    def test_create_imputed_ascwds_job_role_counts(
        self, create_imputed_ascwds_job_role_counts_data
    ):
        expected_lf = pl.LazyFrame(
            create_imputed_ascwds_job_role_counts_data,
            Schemas.create_imputed_ascwds_job_role_counts_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.ascwds_job_role_ratios,
            IndCQC.imputed_ascwds_job_role_ratios,
            IndCQC.imputed_ascwds_job_role_counts,
        )
        returned_lf = job.create_imputed_ascwds_job_role_counts(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.0001)


class TestGetPercentageShareRatios:
    def test_over_groups(self):
        expected_lf = pl.LazyFrame(
            data=[
                (1, "1", date(2026, 1, 1), 1, 0.3333),
                (2, "1", date(2026, 1, 1), 2, 0.6667),
                (3, "1", date(2026, 2, 1), 2, 0.5),
                (4, "1", date(2026, 2, 1), 2, 0.5),
                (5, "2", date(2026, 1, 1), 2, 0.4),
                (6, "2", date(2026, 1, 1), 3, 0.6),
            ],
            schema={
                IndCQC.id_per_locationid_import_date_job_role: pl.Int64,
                IndCQC.location_id: pl.String,
                IndCQC.cqc_location_import_date: pl.Date,
                "vals": pl.Int64,
                "ratios": pl.Float32,
            },
            orient="row",
        )
        input_lf = expected_lf.drop("ratios")
        returned_lf = job.get_percent_share_ratios(
            input_lf, input_col="vals", output_col="ratios"
        ).sort(IndCQC.id_per_locationid_import_date_job_role)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)


class TestCreateASCWDSJobRoleRollingRatio:
    @pytest.mark.parametrize(
        "create_ascwds_job_role_rolling_ratio_data",
        [
            case.as_pytest_param()
            for case in Data.create_ascwds_job_role_rolling_ratio_test_cases
        ],
    )
    def test_create_ascwds_job_role_rolling_ratio(
        self, create_ascwds_job_role_rolling_ratio_data
    ):
        expected_lf = pl.LazyFrame(
            create_ascwds_job_role_rolling_ratio_data,
            Schemas.create_ascwds_job_role_rolling_ratio_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.ascwds_job_role_rolling_ratio,
            IndCQC.estimate_filled_posts_size_group,
        )
        returned_lf = job.create_ascwds_job_role_rolling_ratio(input_lf)
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, rel_tol=0.0001
        )


class TestEstimateFilledPostsSizeGroupExpression:
    def test_estimate_filled_posts_size_group_expression(self):
        expected_lf = pl.LazyFrame(
            data=[
                # non-residential
                (24.999, PrimaryServiceType.non_residential, "NR 1 to 24"),
                (25.0, PrimaryServiceType.non_residential, "NR 25 to 49"),
                (33, PrimaryServiceType.non_residential, "NR 25 to 49"),
                (50.0, PrimaryServiceType.non_residential, "NR 50 to 74"),
                (99.999, PrimaryServiceType.non_residential, "NR 75 to 99"),
                (100.0, PrimaryServiceType.non_residential, "NR 100 plus"),
                # care home only
                (1.0, PrimaryServiceType.care_home_only, "COH 1 to 9"),
                (9.999, PrimaryServiceType.care_home_only, "COH 1 to 9"),
                (10.0, PrimaryServiceType.care_home_only, "COH 10 to 19"),
                (19.999, PrimaryServiceType.care_home_only, "COH 10 to 19"),
                (21.0, PrimaryServiceType.care_home_only, "COH 20 to 29"),
                (30.0, PrimaryServiceType.care_home_only, "COH 30 plus"),
                # care home with nursing
                (1.0, PrimaryServiceType.care_home_with_nursing, "CHWN 1 to 19"),
                (19.999, PrimaryServiceType.care_home_with_nursing, "CHWN 1 to 19"),
                (20.0, PrimaryServiceType.care_home_with_nursing, "CHWN 20 to 29"),
                (29.999, PrimaryServiceType.care_home_with_nursing, "CHWN 20 to 29"),
                (30.0, PrimaryServiceType.care_home_with_nursing, "CHWN 30 plus"),
                # unmatched
                (0.0, PrimaryServiceType.non_residential, None),
                (None, PrimaryServiceType.non_residential, None),
                (25.0, "Other Service", None),
            ],
            schema={
                IndCQC.estimate_filled_posts: pl.Float32,
                IndCQC.primary_service_type: pl.String,
                IndCQC.estimate_filled_posts_size_group: pl.String,
            },
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.estimate_filled_posts_size_group)
        returned_lf = input_lf.with_columns(
            job.estimate_filled_posts_size_group_expression()
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)
