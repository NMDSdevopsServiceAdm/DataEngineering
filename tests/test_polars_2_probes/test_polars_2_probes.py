"""
Throwaway probe tests for ticket 2082 (Polars 2.0 impact analysis). PoC branch only.

Each probe pins today's (polars 1.44.1) behaviour at a call site that the 2.0
upgrade guide or a dependency audit flagged as at risk. They are expected to pass
on 1.44.1; any that fail on 2.0.0rc2 are evidence for the findings doc.

The row-order probes use the real util functions and collect with the default
engine, so on 2.0 they run on the streaming engine. A small streaming chunk size
plus multi-thousand-row input forces many morsels, so an order-sensitive merge
has a real chance to see rows out of order.
"""

from dataclasses import dataclass

import pointblank as pb
import polars as pl
import polars.testing as pl_testing
import pytest

from projects._01_ingest.ascwds.fargate.utils import clean_workplace_utils
from projects._01_ingest.cqc_api.fargate.utils import convert_delta_to_full_utils
from projects._03_independent_cqc._01_filled_posts._02_clean.fargate.utils import (
    clean_ind_cqc_filled_posts_utils,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

ROWS = 50_000
SMALL_CHUNK_SIZE = 500


@pytest.fixture
def small_streaming_chunks():
    """Force many streaming morsels so order-dependent merges are exercised."""
    pl.Config.set_streaming_chunk_size(SMALL_CHUNK_SIZE)
    yield
    pl.Config.set_streaming_chunk_size(None)


class TestCreateFullSnapshot:
    def test_keeps_last_row_per_key_after_concat_under_streaming(
        self, small_streaming_chunks
    ):
        full_lf = pl.LazyFrame(
            {IndCQC.location_id: [str(i) for i in range(ROWS)], "version": "full"}
        )
        delta_lf = pl.LazyFrame(
            {IndCQC.location_id: [str(i) for i in range(ROWS)], "version": "delta"}
        )

        returned_df = convert_delta_to_full_utils.create_full_snapshot(
            full_lf, delta_lf, IndCQC.location_id
        ).collect()

        assert returned_df.height == ROWS
        assert returned_df["version"].unique().to_list() == ["delta"]


class TestDeduplicateCareHomes:
    def test_keeps_first_row_per_key_under_streaming(self, small_streaming_chunks):
        # Two registrations per duplicate key; the earlier registration must be kept.
        keys = [str(i) for i in range(ROWS)]
        input_lf = pl.LazyFrame(
            {
                IndCQC.location_id: [f"{k}_new" for k in keys]
                + [f"{k}_old" for k in keys],
                IndCQC.name: keys + keys,
                IndCQC.care_home: CareHome.care_home,
                IndCQC.imputed_registration_date: [2] * ROWS + [1] * ROWS,
            }
        )

        returned_df = clean_ind_cqc_filled_posts_utils.deduplicate_care_homes(
            input_lf,
            duplicate_columns=[IndCQC.name],
            distinguishing_columns=[IndCQC.imputed_registration_date],
        ).collect()

        assert returned_df.height == ROWS
        assert returned_df[IndCQC.location_id].str.ends_with("_old").all()


class TestRecheckDuplicateEstablishments:
    def test_output_unaffected_by_empty_as_null_default(self):
        # The grouped lists are filtered to duplicate_count > 1 before .explode(),
        # so they can never be empty: the empty_as_null default flip should not
        # change this function's output.
        group = sorted(clean_workplace_utils.DUPLICATE_ESTABLISHMENT_GROUPS[0])
        numeric_row = {
            c: 1 for c in clean_workplace_utils.NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES
        }
        input_lf = pl.LazyFrame(
            [
                {
                    AWPClean.establishment_id: est,
                    AWPClean.import_date: "20260101",
                    **numeric_row,
                }
                for est in group
            ]
        )

        returned_lf = clean_workplace_utils.recheck_duplicate_establishments(input_lf)

        expected_lf = pl.LazyFrame(
            {AWPClean.establishment_id: group, AWPClean.import_date: "20260101"}
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


@dataclass
class ColSchemaMatchTestCase:
    id: str
    schema: pb.Schema
    in_order: bool
    expected_to_pass: bool

    def as_pytest_param(self):
        return pytest.param(
            self.schema, self.in_order, self.expected_to_pass, id=self.id
        )


col_schema_match_cases = [
    # Mirrors validate_cleaned_ind_cqc_data.py: partial schema, fewer columns than the table.
    ColSchemaMatchTestCase(
        id="records_passing_partial_col_schema_match_without_raising",
        schema=pb.Schema(columns={"a": "String"}),
        in_order=False,
        expected_to_pass=True,
    ),
    # Mirrors validate_01_merge_metadata.py / validate_archive_job_role_estimates.py.
    ColSchemaMatchTestCase(
        id="records_passing_partial_col_schema_match_without_raising_in_order",
        schema=pb.Schema(columns={"a": "String", "b": "Int64"}),
        in_order=True,
        expected_to_pass=True,
    ),
    ColSchemaMatchTestCase(
        id="records_failed_col_schema_match_step_without_raising_any_order",
        schema=pb.Schema(columns={"a": "String", "missing": "String"}),
        in_order=False,
        expected_to_pass=False,
    ),
    ColSchemaMatchTestCase(
        id="records_failed_col_schema_match_step_without_raising_in_order",
        schema=pb.Schema(columns={"a": "String", "b": "Int64", "missing": "String"}),
        in_order=True,
        expected_to_pass=False,
    ),
]


class TestPointblankColSchemaMatch:
    @pytest.mark.parametrize(
        "schema, in_order, expected_to_pass",
        [c.as_pytest_param() for c in col_schema_match_cases],
    )
    def test_step_is_recorded(self, schema, in_order, expected_to_pass):
        input_lf = pl.LazyFrame({"a": ["x"], "b": [1], "c": [1.0]})

        validation = (
            pb.Validate(data=input_lf)
            .col_schema_match(schema=schema, complete=False, in_order=in_order)
            .interrogate()
        )

        assert validation.validation_info[0].all_passed is expected_to_pass


class TestPointblankColValsInSet:
    def test_col_vals_in_set_on_enum_column_does_not_raise(self):
        enum_type = pl.Enum(["Y", "N"])
        input_lf = pl.LazyFrame(
            {IndCQC.care_home: ["Y", "N"]}, schema={IndCQC.care_home: enum_type}
        )

        validation = (
            pb.Validate(data=input_lf)
            .col_vals_in_set(columns=IndCQC.care_home, set=["Y", "N"])
            .interrogate()
        )

        assert validation.validation_info[0].all_passed is True
