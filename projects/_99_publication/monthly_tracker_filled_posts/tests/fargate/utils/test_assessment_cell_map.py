import pytest
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as job,
)
from utils.column_names.publication_columns import PublicationColumns as Pub

sheet_params = [pytest.param(s, id=s.name) for s in job.ASSESSMENT_SHEETS]


def _cells(ranges: list[str]) -> set[tuple[int, int]]:
    return {
        (row, col)
        for cells in ranges
        for row, col in CellRange(cells).cells  # type: ignore[attr-defined]
    }


COMPARISON_PERC_CHANGE_SOURCES = [
    pytest.param(
        job.COMP_SHORT_TERM,
        Pub.filtered_short_sfc_cum_pct_change,
        Pub.filtered_short_ct_cum_pct_change,
        id="short_term",
    ),
    pytest.param(
        job.COMP_MEDIUM_TERM,
        Pub.filtered_medium_sfc_cum_pct_change,
        Pub.filtered_medium_ct_cum_pct_change,
        id="medium_term",
    ),
    pytest.param(
        job.COMP_LONG_TERM,
        Pub.filtered_long_sfc_cum_pct_change,
        Pub.filtered_long_ct_cum_pct_change,
        id="long_term",
    ),
    pytest.param(
        job.NO_FILTERS,
        Pub.unfiltered_long_sfc_cum_pct_change,
        Pub.unfiltered_long_ct_cum_pct_change,
        id="no_filters",
    ),
]


class TestAssessmentSheets:
    @pytest.mark.parametrize("sheet", sheet_params)
    def test_input_range_keys_are_unique_on_a_sheet(self, sheet: job.SheetMap):
        """The writer finds a range by key, so a key names one range."""
        keys = [r.key for r in sheet.input_ranges if r.key]

        assert len(keys) == len(set(keys))

    @pytest.mark.parametrize("sheet", sheet_params)
    def test_input_ranges_do_not_overlap(self, sheet: job.SheetMap):
        """Each cell has one input range, so the writer fills it from one place."""
        ranges = [r.cells for r in sheet.input_ranges]

        assert sum(len(_cells([r])) for r in ranges) == len(_cells(ranges))

    @pytest.mark.parametrize(
        "note, source_column",
        [
            pytest.param(
                "care_homes.filled_posts",
                Pub.unfiltered_filled_posts,
                id="care_homes_filled_posts",
            ),
            pytest.param(
                "non_res.filled_posts",
                Pub.unfiltered_filled_posts,
                id="non_res_filled_posts",
            ),
            pytest.param(
                "care_homes.locations_by_period",
                Pub.unfiltered_locationid_count,
                id="care_homes_locations",
            ),
            pytest.param(
                "non_res.locations_by_period",
                Pub.unfiltered_locationid_count,
                id="non_res_locations",
            ),
            pytest.param(
                "care_homes.ct",
                Pub.unfiltered_long_ct_total_employed,
                id="care_homes_capacity_tracker",
            ),
            pytest.param(
                "non_res.ct",
                Pub.unfiltered_long_ct_total_employed,
                id="non_res_capacity_tracker",
            ),
        ],
    )
    def test_no_filters_sheet_sources(self, note: str, source_column: str | None):
        """No filters reads the unfiltered filled posts and Capacity Tracker posts columns."""
        sources = {r.key: r.source_column for r in job.NO_FILTERS.input_ranges}

        assert sources[note] == source_column

    @pytest.mark.parametrize(
        "sheet, filled_posts_source, ct_source", COMPARISON_PERC_CHANGE_SOURCES
    )
    def test_comparison_sheet_perc_change_sources(
        self, sheet: job.SheetMap, filled_posts_source: str, ct_source: str
    ):
        """Each comparison sheet's % change columns read its own term's columns."""
        sources = {r.key: r.source_column for r in sheet.input_ranges}

        assert [
            sources[note]
            for note in (
                "care_homes.filled_posts_change",
                "care_homes.ct_change",
                "non_res.filled_posts_change",
                "non_res.ct_change",
            )
        ] == [filled_posts_source, ct_source, filled_posts_source, ct_source]
