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


COMPARISON_SHEETS = [
    job.COMP_SHORT_TERM,
    job.COMP_MEDIUM_TERM,
    job.COMP_LONG_TERM,
    job.NO_FILTERS,
]


def _cells_as_coordinates(sheet: job.SheetMap) -> set[str]:
    return {
        CellRange(min_row=row, min_col=col, max_row=row, max_col=col).coord
        for row, col in _cells([r.cells for r in sheet.input_ranges])
    }


class TestAssessmentSheets:
    @pytest.mark.parametrize("sheet", sheet_params)
    def test_input_ranges_do_not_overlap(self, sheet: job.SheetMap):
        """Each cell has one input range, so the writer fills it from one place."""
        ranges = [r.cells for r in sheet.input_ranges]

        assert sum(len(_cells([r])) for r in ranges) == len(_cells(ranges))

    @pytest.mark.parametrize("sheet", COMPARISON_SHEETS, ids=lambda s: s.name)
    @pytest.mark.parametrize("cell", ["E27", "N27"])
    def test_cumulative_heading_cells_are_not_inputs(
        self, sheet: job.SheetMap, cell: str
    ):
        """The '% change' headings are fixed template text on comparison sheets."""
        assert cell not in _cells_as_coordinates(sheet)

    @pytest.mark.parametrize(
        "sheet, cell",
        [
            pytest.param(job.COMP_LONG_TERM, "B7", id="comp_long_heading"),
            pytest.param(job.COMP_LONG_TERM, "B14", id="comp_long_note"),
            pytest.param(job.NO_FILTERS, "B7", id="no_filters_heading"),
        ],
    )
    def test_long_term_text_cells_are_not_inputs(self, sheet: job.SheetMap, cell: str):
        """The long-term sheets' B7 heading and B14 note are fixed template text."""
        assert cell not in _cells_as_coordinates(sheet)

    @pytest.mark.parametrize(
        "sheet", [job.COMP_SHORT_TERM, job.COMP_MEDIUM_TERM], ids=lambda s: s.name
    )
    def test_short_and_medium_term_keep_dated_text_inputs(self, sheet: job.SheetMap):
        """The B7 heading and B14 note still carry a written date."""
        assert {"B7", "B14"} <= _cells_as_coordinates(sheet)

    @pytest.mark.parametrize(
        "note, source_column",
        [
            pytest.param(
                "care homes SfC",
                Pub.filled_posts,
                id="care_homes_filled_posts",
            ),
            pytest.param("non-res SfC", Pub.filled_posts, id="non_res_filled_posts"),
            pytest.param(
                "care homes number of locations by period",
                Pub.locationid_count,
                id="care_homes_locations",
            ),
            pytest.param(
                "non-res number of locations by period",
                Pub.locationid_count,
                id="non_res_locations",
            ),
            pytest.param(
                "care homes CT",
                Pub.unfiltered_ct_total_employed_long,
                id="care_homes_capacity_tracker",
            ),
            pytest.param(
                "non-res CT",
                Pub.unfiltered_ct_total_employed_long,
                id="non_res_capacity_tracker",
            ),
        ],
    )
    def test_no_filters_sheet_sources(self, note: str, source_column: str | None):
        """No filters reads the unfiltered filled posts and Capacity Tracker posts columns."""
        sources = {r.note: r.source_column for r in job.NO_FILTERS.input_ranges}

        assert sources[note] == source_column
