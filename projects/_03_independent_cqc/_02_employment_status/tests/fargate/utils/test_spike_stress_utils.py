from datetime import date, timedelta

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_stress_utils as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

SOURCE_COLUMN = EmpStatus.permanent_percentage
ROW_COUNT = 300


def build_test_lf() -> pl.LazyFrame:
    # Every third row null, mimicking the real all-or-nothing null pattern.
    return pl.LazyFrame(
        {
            IndCQC.location_id: [f"loc{i}" for i in range(ROW_COUNT)],
            IndCQC.cqc_location_import_date: [
                date(2024, 1, 1) + timedelta(days=i % 30) for i in range(ROW_COUNT)
            ],
            SOURCE_COLUMN: [None if i % 3 == 0 else 0.5 for i in range(ROW_COUNT)],
        }
    )


class TestAddDummyEmploymentStatusColumns:
    def test_adds_requested_number_of_dummy_columns(self):
        returned_lf, _ = job.add_dummy_employment_status_columns(
            build_test_lf(), dummy_count=15, null_pattern_source_column=SOURCE_COLUMN
        )

        added_columns = set(returned_lf.collect_schema().names()) - set(
            build_test_lf().collect_schema().names()
        )
        assert len(added_columns) == 15

    def test_dummy_columns_null_on_same_rows_as_source_column(self):
        returned_lf, mapping = job.add_dummy_employment_status_columns(
            build_test_lf(), dummy_count=3, null_pattern_source_column=SOURCE_COLUMN
        )
        returned_df = returned_lf.collect()

        expected_is_null = returned_df[SOURCE_COLUMN].is_null().to_list()
        for column in mapping.values():
            assert returned_df[column].is_null().to_list() == expected_is_null

    def test_dummy_values_are_between_zero_and_one(self):
        returned_lf, mapping = job.add_dummy_employment_status_columns(
            build_test_lf(), dummy_count=3, null_pattern_source_column=SOURCE_COLUMN
        )
        returned_df = returned_lf.select(list(mapping.values())).collect()

        for column in mapping.values():
            values = returned_df[column].drop_nulls()
            assert returned_df[column].dtype == pl.Float32
            assert values.min() >= 0.0
            assert values.max() < 1.0

    def test_returns_label_to_column_mapping_for_dummies(self):
        returned_lf, mapping = job.add_dummy_employment_status_columns(
            build_test_lf(), dummy_count=2, null_pattern_source_column=SOURCE_COLUMN
        )

        assert mapping == {
            "dummy_status_01": "dummy_status_01_percentage",
            "dummy_status_02": "dummy_status_02_percentage",
        }
        assert set(mapping.values()) <= set(returned_lf.collect_schema().names())
