import unittest
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import polars_utils.cleaning_utils as job
from tests.test_polars_utils_data import CleaningUtilsData as Data
from tests.test_polars_utils_schemas import CleaningUtilsSchemas as Schemas
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


class PolarsCleaningUtilsTests(unittest.TestCase):
    def setUp(self):
        pass


class AddAlignedDateColumnsTests(PolarsCleaningUtilsTests):
    def setUp(self):
        self.primary_column = CQCLClean.cqc_location_import_date
        self.secondary_column = AWPClean.ascwds_workplace_import_date

    def test_mix_of_misaligned_dates_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_rows, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_primary_with_secondary_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_exact_match_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_exact_match_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_exact_match_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_closest_historical_date_joined_correctly(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_closest_historical_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_closest_historical_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_future_date_only_returns_null_secondary_column(self):
        primary_lf = pl.LazyFrame(
            Data.align_dates_primary_single_row, Schemas.align_dates_primary_schema
        )
        secondary_lf = pl.LazyFrame(
            Data.align_dates_secondary_future_rows,
            Schemas.align_dates_secondary_schema,
        )
        returned_lf = job.add_aligned_date_column(
            primary_lf,
            secondary_lf,
            self.primary_column,
            self.secondary_column,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_align_dates_secondary_future_rows,
            Schemas.expected_merged_dates_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )


class TestApplyCategoricalLabels:
    labels_lf = pl.LazyFrame(Data.labels_data, Schemas.labels_schema, orient="row")
    test_defaults_lf = pl.LazyFrame(
        {
            AWK.worker_id: ["1", "2", "3", "4"],
            AWK.gender: ["1", "2", None, "2"],
        }
    )
    expected_defaults_lf = pl.LazyFrame(
        {
            AWK.worker_id: ["1", "2", "3", "4"],
            AWK.gender: ["1", "2", None, "2"],
            "gender_labels": ["male", "female", None, "female"],
        }
    )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.apply_catagorical_labels_test_cases
        ],
    )
    def test_apply_categorical_labels(self, case):
        test_lf = pl.LazyFrame(case.test_data)

        expected_lf = pl.LazyFrame(
            case.expected_data,
        )

        returned_lf = job.apply_categorical_labels(
            test_lf,
            self.labels_lf,
            case.column_names,
            case.add_as_new_column,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )

    def test_add_as_new_column_defaults_to_true(self):
        returned_lf = job.apply_categorical_labels(
            self.test_defaults_lf,
            self.labels_lf,
            column_names=[AWK.gender],
        )
        pl_testing.assert_frame_equal(
            returned_lf,
            self.expected_defaults_lf,
            check_row_order=False,
        )

    def test_raises_value_error_when_column_not_in_dataframe(
        self,
    ):
        test_error_lf = pl.LazyFrame(
            data={
                AWK.worker_id: ["1"],
                AWK.gender: ["1"],
            },
        )
        error_msg = "Column age not found in LazyFrame."
        with pytest.raises(ValueError, match=error_msg):
            job.apply_categorical_labels(
                test_error_lf,
                self.labels_lf,
                [AWK.age],
            )

    def test_raises_key_error_when_column_not_in_labels_dict(
        self,
    ):
        test_error_lf = pl.LazyFrame(
            data={
                AWK.worker_id: ["1"],
                AWK.gender: ["1"],
            },
        )
        error_msg = "No label mapping found for gender."
        with pytest.raises(KeyError, match=error_msg):
            job.apply_categorical_labels(
                test_error_lf,
                self.labels_lf.filter(pl.col("column_name") != AWK.gender),
                [AWK.gender],
            )


class ColumnToDateTests(unittest.TestCase):
    def test_converts_date_string_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_without_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_integer_without_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_integer_without_hyphens_rows,
            schema=Schemas.col_to_date_integer_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_converts_date_string_with_hyphens_to_date(self):
        lf = pl.LazyFrame(
            data=Data.column_to_date_string_with_hyphens_rows,
            schema=Schemas.col_to_date_string_schema,
        )

        returned_lf = job.column_to_date(lf, "date_col")

        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_rows,
            schema=Schemas.expected_col_to_date_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_adds_a_new_column_with_converted_date(self):
        expected_lf = pl.LazyFrame(
            data=Data.expected_column_to_date_with_new_col_rows,
            schema=Schemas.expected_col_to_date_with_new_col_schema,
        )
        returned_lf = job.column_to_date(
            expected_lf.drop("new_date_col"), "date_col", "new_date_col"
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CalculateFilledPostsPerBedRatioTests(unittest.TestCase):
    def test_calculate_filled_posts_per_bed_ratio(self):
        expected_lf = pl.LazyFrame(
            Data.expected_filled_posts_per_bed_ratio_rows,
            Schemas.expected_filled_posts_per_bed_ratio_schema,
            orient="row",
        )
        returned_lf = job.calculate_filled_posts_per_bed_ratio(
            expected_lf.drop(IndCQC.filled_posts_per_bed_ratio),
            IndCQC.ascwds_filled_posts_dedup,
            IndCQC.filled_posts_per_bed_ratio,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class ReduceDatasetToEarliestFilePerMonthTests(unittest.TestCase):
    def test_reduce_dataset_to_earliest_file_per_month_returns_correct_rows(self):
        test_lf = pl.LazyFrame(
            Data.reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        returned_lf = job.reduce_dataset_to_earliest_file_per_month(test_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_reduce_dataset_to_earliest_file_per_month_rows,
            Schemas.reduce_dataset_to_earliest_file_per_month_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CreateBandedBedCountColumnTests(unittest.TestCase):
    def test_create_banded_bed_count_column(self):
        expected_lf = pl.LazyFrame(
            Data.expected_create_banded_bed_count_column_rows,
            Schemas.expected_create_banded_bed_count_column_schema,
            orient="row",
        )
        test_splits = [0, 1, 25, float("Inf")]
        returned_lf = job.create_banded_bed_count_column(
            expected_lf.drop(IndCQC.number_of_beds_banded),
            IndCQC.number_of_beds_banded,
            test_splits,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CastDateStringsToDatesTests(unittest.TestCase):
    def test_string_cols_containing_identifier_are_converted_to_date_apart_from_import_date(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=[("01/01/2026", "02/01/2026", "02/01/2026")],
            schema={
                "import_date": pl.String,
                "any_other_date_col": pl.String,
                "another_date_column": pl.String,
            },
            orient="row",
        )
        returned_lf = job.cast_date_strings_to_dates(test_lf)
        expected_lf = pl.LazyFrame(
            data=[("01/01/2026", date(2026, 1, 2), date(2026, 1, 2))],
            schema={
                "import_date": pl.String,
                "any_other_date_col": pl.Date,
                "another_date_column": pl.Date,
            },
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_date_strings_which_differ_to_raw_date_format_are_converted_to_null(self):
        input_lf = pl.LazyFrame(
            {
                "time_element_date": [
                    "01/01/2026",        # valid
                    "03/01/2026 00:00",  # invalid format
                    "2026/01/03",        # wrong format
                    "",                  # empty
                    None,                # already null
                ]
            }
        ) # fmt: skip
        result_lf = job.cast_date_strings_to_dates(input_lf, raw_date_format="%d/%m/%Y")
        expected_lf = pl.LazyFrame(
            {
                "time_element_date": [
                    date(2026, 1, 1),
                    None,
                    None,
                    None,
                    None,
                ]
            }
        )

        pl_testing.assert_frame_equal(result_lf, expected_lf)

    def test_column_is_not_converted_if_already_date_type(self):
        input_lf = pl.LazyFrame(
            {
                "time_element_date": [
                    date(2026, 1, 1),
                    date(2026, 1, 2),
                    None,
                ]
            }
        )
        result_lf = job.cast_date_strings_to_dates(input_lf)

        pl_testing.assert_frame_equal(result_lf, input_lf)


@dataclass
class MergeJobRoleColumnsTestCase:
    id: str
    mapping: dict[str, list[str]]
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


class TestMergeJobRoleColumns:
    merge_job_role_columns_test_cases = [
        MergeJobRoleColumnsTestCase(
            id="merges_one_role_to_one_role",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="merges_many_roles_to_one_role",
            mapping={"01": ["02", "03"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_03_employees: 3,
            },
            expected_data={
                AWPClean.job_role_01_employees: 6,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="ignores_null_values_in_sum",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: None,
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="returns_null_when_all_roles_are_null",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: None,
                AWPClean.job_role_02_employees: None,
            },
            expected_data={
                AWPClean.job_role_01_employees: None,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="ignores_roles_in_mapping_but_not_in_input_data",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="creates_target_role_when_missing",
            mapping={"99": ["01", "02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
            },
            expected_data={
                "jr99emp": 3,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="merges_all_matching_suffixes",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_01_starters: 3,
                AWPClean.job_role_02_starters: 4,
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
                AWPClean.job_role_01_starters: 7,
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="handles_extra_columns_in_input_data",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                "not_a_job_role_column": "A",
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
                "not_a_job_role_column": "A",
            },
        ),
        MergeJobRoleColumnsTestCase(
            id="handles_job_roles_with_same_characters",
            mapping={"101": ["10"]},
            input_data={
                AWPClean.job_role_10_employees: 10,
            },
            expected_data={
                "jr101emp": 10,
            },
        ),
    ]

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in merge_job_role_columns_test_cases],
    )
    def test_merge_job_role_columns(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = pl.LazyFrame(case.expected_data)
        returned_lf = job.merge_job_role_columns(test_lf, case.mapping)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
