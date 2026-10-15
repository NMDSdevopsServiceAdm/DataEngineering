import pytest
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as job,
)
from utils.column_names.publication_columns import PublicationColumns as Pub

sheet_params = [pytest.param(s, id=s.name) for s in job.DIAGNOSTICS_SHEETS]


def _cells(ranges: list[str]) -> set[tuple[int, int]]:
    return {
        (row, col)
        for cells in ranges
        for row, col in CellRange(cells).cells  # type: ignore[attr-defined]
    }


class TestDiagnosticsSheets:
    @pytest.mark.parametrize("sheet", sheet_params)
    def test_input_ranges_do_not_overlap(self, sheet: job.SheetMap):
        """Each cell has one input range, so the writer fills it from one place."""
        ranges = [r.cells for r in sheet.input_ranges]

        assert sum(len(_cells([r])) for r in ranges) == len(_cells(ranges))

    @pytest.mark.parametrize(
        "note, source_column",
        [
            pytest.param(
                "care homes SfC",
                Pub.publication_filled_posts,
                id="care_homes_filled_posts",
            ),
            pytest.param(
                "non-res SfC", Pub.publication_filled_posts, id="non_res_filled_posts"
            ),
            pytest.param(
                "care homes number of locations by period (needs a new pipeline column)",
                None,
                id="care_homes_locations",
            ),
            pytest.param(
                "non-res number of locations by period (needs a new pipeline column)",
                None,
                id="non_res_locations",
            ),
            pytest.param("care homes CT", None, id="care_homes_capacity_tracker"),
            pytest.param("non-res CT", None, id="non_res_capacity_tracker"),
        ],
    )
    def test_no_filters_sheet_sources(self, note: str, source_column: str | None):
        """No filters reads publication filled posts; the other columns need new pipeline columns."""
        sources = {r.note: r.source_column for r in job.NO_FILTERS.input_ranges}

        assert sources[note] == source_column
