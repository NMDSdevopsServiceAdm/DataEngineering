import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ImputeJobRoleData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ImputeJobRoleSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils"


class TestPercentageShareHandlingZeroSum:
    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(
                [5.0, 2.0, 1.0],
                [0.625, 0.25, 0.125],
                id="when_all_values_present",
            ),
            pytest.param(
                [0, 0],
                [0.5, 0.5],
                id="handles_zero_sum_case_with_even_distribution",
            ),
            pytest.param(
                [0, None, 0, None],
                [0.5, None, 0.5, None],
                id="handles_zero_sum_case_with_even_distribution_across_non_nulls",
            ),
        ],
    )
    def test_percentage_share_handling_zero_sum(self, input_, expected):
        input_lf = pl.LazyFrame({"values": input_})
        expected_lf = pl.LazyFrame({"pct_share": expected})
        returned_lf = input_lf.select(
            job.percentage_share_handling_zero_sum("values").alias("pct_share")
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddJobRoleGroupsColumn:
    job_group_col = "job_group_col"

    def test_add_job_role_groups_column_maps_roles_correctly(self):
        expected_data = [
            (job, group)
            for job, group in zip(
                AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.keys(),
                AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.values(),
            )
        ]
        expected_schema = {
            IndCQC.main_job_role_clean_labelled: job.CategoricalColumnTypes.JobRoleEnumType,
            self.job_group_col: job.CategoricalColumnTypes.JobGroupEnumType,
        }
        expected_lf = pl.LazyFrame(
            data=expected_data, schema=expected_schema, orient="row"
        )

        test_lf = expected_lf.drop(self.job_group_col)
        returned_lf = job.add_job_role_groups_column(test_lf, "job_group_col")
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
