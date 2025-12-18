import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_merge.fargate.utils.merge_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    MergeUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    MergeUtilsSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.utils.merge_utils"


class MergeUtilsTests(unittest.TestCase):

    def setUp(self) -> None:
        self.test_clean_cqc_location_lf = pl.LazyFrame(
            Data.clean_cqc_location_for_merge_rows,
            Schemas.clean_cqc_location_for_merge_schema,
            orient="row",
        )
        self.test_data_with_care_home_col = pl.LazyFrame(
            Data.data_to_merge_with_care_home_col_rows,
            Schemas.data_to_merge_with_care_home_col_schema,
            orient="row",
        )
        self.test_data_without_care_home_col = pl.LazyFrame(
            Data.data_to_merge_without_care_home_col_rows,
            Schemas.data_to_merge_without_care_home_col_schema,
            orient="row",
        )

    def test_join_data_into_cqc_lf_returns_expected_data_when_care_home_column_not_required(
        self,
    ):
        returned_lf = job.join_data_into_cqc_lf(
            self.test_clean_cqc_location_lf,
            self.test_data_without_care_home_col,
            AWPClean.location_id,
            AWPClean.ascwds_workplace_import_date,
        )

        expected_merged_lf = pl.LazyFrame(
            Data.expected_merged_without_care_home_col_rows,
            Schemas.expected_merged_without_care_home_col_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_merged_lf)

    def test_join_data_into_cqc_lf_returns_expected_data_when_care_home_column_is_required(
        self,
    ):
        returned_lf = job.join_data_into_cqc_lf(
            self.test_clean_cqc_location_lf,
            self.test_data_with_care_home_col,
            CQCPIRClean.location_id,
            CQCPIRClean.cqc_pir_import_date,
            CQCPIRClean.care_home,
        )

        expected_merged_lf = pl.LazyFrame(
            Data.expected_merged_with_care_home_col_rows,
            Schemas.expected_merged_with_care_home_col_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_merged_lf)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
