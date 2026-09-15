import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate.utils.utils as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate.utils.utils"


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
            IndCQC.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleCatType,
            self.job_group_col: CategoricalColumnTypes.JobGroupCatType,
        }
        expected_lf = pl.LazyFrame(
            data=expected_data, schema=expected_schema, orient="row"
        )

        test_lf = expected_lf.drop(self.job_group_col)
        returned_lf = job.add_job_role_groups_column(test_lf, "job_group_col")
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
