from dataclasses import dataclass
from unittest.mock import Mock, patch

import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.merge_utils as job

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate.utils.merge_utils"
)


@dataclass
class ParseRunNumberTestCase:
    id: str
    value: str | None
    expected: int | None

    def as_pytest_param(self):
        return pytest.param(self.value, self.expected, id=self.id)


parse_run_number_cases = [
    ParseRunNumberTestCase(id="returns_none_when_not_given", value=None, expected=None),
    ParseRunNumberTestCase(id="returns_none_for_latest", value="latest", expected=None),
    ParseRunNumberTestCase(id="returns_int_for_digit_string", value="3", expected=3),
]


class TestParseRunNumber:
    @pytest.mark.parametrize(
        "value, expected", [c.as_pytest_param() for c in parse_run_number_cases]
    )
    def test_returns_expected_run_number(self, value: str | None, expected: int | None):
        assert job.parse_run_number(value) == expected

    @pytest.mark.parametrize("value", ["abc", "null", "-1", "", "²"])
    def test_raises_for_non_numeric_value(self, value: str):
        with pytest.raises(ValueError, match="run_number"):
            job.parse_run_number(value)


@dataclass
class SelectRunNumberTestCase:
    id: str
    runs_by_root: dict[str, dict[int, str]]
    run_number: int | None
    expected: int | None = None
    expected_error: str | None = None

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


BOTH_ROOTS_RUNS_1_TO_3 = {
    "a": {1: "2026-09-22", 2: "2026-10-01", 3: "2026-10-01"},
    "b": {1: "2026-09-22", 2: "2026-10-01", 3: "2026-10-01"},
}

select_run_number_cases = [
    SelectRunNumberTestCase(
        id="returns_highest_run_number_when_none_requested",
        runs_by_root=BOTH_ROOTS_RUNS_1_TO_3,
        run_number=None,
        expected=3,
    ),
    SelectRunNumberTestCase(
        id="returns_requested_run_number",
        runs_by_root=BOTH_ROOTS_RUNS_1_TO_3,
        run_number=2,
        expected=2,
    ),
    SelectRunNumberTestCase(
        id="returns_requested_run_number_when_a_later_run_exists",
        runs_by_root=BOTH_ROOTS_RUNS_1_TO_3,
        run_number=1,
        expected=1,
    ),
    SelectRunNumberTestCase(
        id="raises_when_requested_run_is_missing_from_a_root",
        runs_by_root={"a": {1: "2026-09-22", 2: "2026-10-01"}, "b": {1: "2026-09-22"}},
        run_number=2,
        expected_error="run_number 2 not found in b",
    ),
    SelectRunNumberTestCase(
        id="raises_when_a_root_has_no_runs",
        runs_by_root={"a": {1: "2026-09-22"}, "b": {}},
        run_number=None,
        expected_error="No archived runs found in b",
    ),
    SelectRunNumberTestCase(
        id="raises_when_roots_disagree_on_the_latest_run",
        runs_by_root={"a": {1: "2026-09-22", 2: "2026-10-01"}, "b": {1: "2026-09-22"}},
        run_number=None,
        expected_error="latest run differs",
    ),
]


class TestSelectRunNumber:
    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in select_run_number_cases
            if c.expected is not None
        ],
    )
    def test_returns_expected_run_number(self, case: SelectRunNumberTestCase):
        assert (
            job.select_run_number(case.runs_by_root, case.run_number) == case.expected
        )

    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in select_run_number_cases
            if c.expected_error is not None
        ],
    )
    def test_raises_for_unusable_runs(self, case: SelectRunNumberTestCase):
        with pytest.raises(ValueError, match=case.expected_error):
            job.select_run_number(case.runs_by_root, case.run_number)


class TestResolveRunSources:
    estimates_root = "s3://bucket/dataset=estimates/"
    metadata_root = "s3://bucket/dataset=metadata/"
    runs = {1: "2026-09-22", 2: "2026-10-01"}

    @patch(f"{PATCH_PATH}.list_archive_runs")
    def test_returns_latest_run_sources_when_none_requested(
        self, list_archive_runs_mock: Mock
    ):
        list_archive_runs_mock.return_value = self.runs

        returned = job.resolve_run_sources(
            [self.estimates_root, self.metadata_root], None
        )

        assert returned == [
            "s3://bucket/dataset=estimates/archive_date=2026-10-01/run_number=2/",
            "s3://bucket/dataset=metadata/archive_date=2026-10-01/run_number=2/",
        ]

    @patch(f"{PATCH_PATH}.list_archive_runs")
    def test_returns_requested_run_sources(self, list_archive_runs_mock: Mock):
        list_archive_runs_mock.return_value = self.runs

        returned = job.resolve_run_sources([self.estimates_root, self.metadata_root], 1)

        assert returned == [
            "s3://bucket/dataset=estimates/archive_date=2026-09-22/run_number=1/",
            "s3://bucket/dataset=metadata/archive_date=2026-09-22/run_number=1/",
        ]

    @patch(f"{PATCH_PATH}.list_archive_runs")
    def test_uses_each_roots_own_archive_date(self, list_archive_runs_mock: Mock):
        list_archive_runs_mock.side_effect = [{2: "2026-10-01"}, {2: "2026-10-02"}]

        returned = job.resolve_run_sources([self.estimates_root, self.metadata_root], 2)

        assert returned == [
            "s3://bucket/dataset=estimates/archive_date=2026-10-01/run_number=2/",
            "s3://bucket/dataset=metadata/archive_date=2026-10-02/run_number=2/",
        ]

    @pytest.mark.parametrize(
        "root", ["s3://bucket/dataset=a/", "s3://bucket/dataset=a"]
    )
    @patch(f"{PATCH_PATH}.list_archive_runs")
    def test_builds_same_source_with_or_without_trailing_slash(
        self, list_archive_runs_mock: Mock, root: str
    ):
        list_archive_runs_mock.return_value = self.runs

        returned = job.resolve_run_sources([root], 2)

        assert returned == [
            "s3://bucket/dataset=a/archive_date=2026-10-01/run_number=2/"
        ]
