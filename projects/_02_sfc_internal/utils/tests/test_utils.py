from dataclasses import dataclass

import polars as pl
import pytest

import projects._02_sfc_internal.utils.utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_values.categorical_column_values import ParentsOrSinglesAndSubs


@dataclass
class ParentsOrSinglesAndSubsTestCase:
    id: str
    is_parent: str
    parent_permission: str
    expected: str

    def as_pytest_param(self):
        return pytest.param(
            self.is_parent, self.parent_permission, self.expected, id=self.id
        )


parents_or_singles_cases = [
    ParentsOrSinglesAndSubsTestCase(
        "is_parent_yes_is_a_parent",
        "Yes",
        "Parent has ownership",
        ParentsOrSinglesAndSubs.parents,
    ),
    ParentsOrSinglesAndSubsTestCase(
        "is_parent_yes_with_workplace_ownership_is_still_a_parent",
        "Yes",
        "Workplace has ownership",
        ParentsOrSinglesAndSubs.parents,
    ),
    ParentsOrSinglesAndSubsTestCase(
        "is_not_parent_with_parent_ownership_is_a_parent",
        "No",
        "Parent has ownership",
        ParentsOrSinglesAndSubs.parents,
    ),
    ParentsOrSinglesAndSubsTestCase(
        "is_not_parent_with_workplace_ownership_is_singles_and_subs",
        "No",
        "Workplace has ownership",
        ParentsOrSinglesAndSubs.singles_and_subs,
    ),
]


class TestAddParentsOrSinglesAndSubsColumn:
    @pytest.mark.parametrize(
        "is_parent, parent_permission, expected",
        [c.as_pytest_param() for c in parents_or_singles_cases],
    )
    def test_classifies_account_correctly(self, is_parent, parent_permission, expected):
        input_lf = pl.LazyFrame(
            {
                AWPClean.is_parent: [is_parent],
                AWPClean.parent_permission: [parent_permission],
            }
        )

        returned_lf = job.add_parents_or_singles_and_subs_column(input_lf)

        assert returned_lf.collect()[
            ReconColumn.parents_or_singles_and_subs
        ].to_list() == [expected]
