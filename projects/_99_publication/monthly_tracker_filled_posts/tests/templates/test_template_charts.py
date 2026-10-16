"""The template's charts cover the right cells, read well, and skip unfilled slots."""

import zipfile

import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates.template_support import (
    NS,
)

# Filled posts % change charts: the title names the column plotted.
FILLED_POSTS_CHART_COLUMNS = {
    "All CQC locations": {"J"},
    "CQC care homes": {"K"},
    "CQC non-residential": {"N"},
}


class TestChartContents:
    @pytest.mark.parametrize("sheet", support.sheet_params)
    def test_chart_count_matches_cell_map(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """Each sheet has the number of charts the cell map expects."""
        assert len(support.charts(package, sheet.name)) == sheet.chart_count

    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_series_refs_resolve_to_cell_map_ranges(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """Charts only read cells the cell map lists."""
        mapped_cells = {
            s.name: support.cells_in(
                [r.cells for r in s.input_ranges] + list(s.chart_label_ranges)
            )
            for s in cell_map.DIAGNOSTICS_SHEETS
        }
        unmapped_refs = []
        for chart in support.charts(package, sheet.name):
            for formula in chart.findall(".//c:f", NS):
                ref_sheet, ref_cells = support.split_reference(formula.text)
                if not support.cells_in([ref_cells]) <= mapped_cells.get(
                    ref_sheet, set()
                ):
                    unmapped_refs.append(formula.text)

        assert unmapped_refs == [], "Chart references outside the cell map"

    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_series_span_the_full_window(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """Every series covers all the slots, so a new month never falls outside."""
        wrong_height = {
            formula.text
            for chart in support.charts(package, sheet.name)
            for formula in chart.findall(".//c:ser//c:cat//c:f", NS)
            + chart.findall(".//c:ser//c:val//c:f", NS)
            if support.row_count(formula.text) != sheet.window_rows
        }

        assert wrong_height == set(), f"Series not spanning {sheet.window_rows} rows"

    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_series_in_a_chart_share_one_category_range(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """Both series share the horizontal axis, so they use the same dates."""
        mixed = [
            index
            for index, chart in enumerate(support.charts(package, sheet.name))
            if len({f.text for f in chart.findall(".//c:ser/c:cat//c:f", NS)}) != 1
        ]

        assert mixed == [], "Charts whose series use different date ranges"

    @pytest.mark.parametrize("sheet", support.comparison_sheet_params)
    def test_series_names_come_from_the_header_above_their_values(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """The legend follows the column headings it sits above."""
        wrong_name_cell = []
        for chart in support.charts(package, sheet.name):
            for series in chart.findall(".//c:ser", NS):
                name = support.split_reference(series.find("c:tx//c:f", NS).text)[1]
                values_ref = series.find("c:val//c:f", NS).text
                header_cell = (
                    f"{support.column_of(values_ref)}"
                    f"{support.rows_of(values_ref)[0] - 1}"
                )
                if name != header_cell:
                    wrong_name_cell.append((name, values_ref))

        assert wrong_name_cell == [], "Series named from a different column"

    @pytest.mark.parametrize("sheet", support.comparison_sheet_params)
    def test_comparison_chart_titles_match_their_series(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """A chart is titled for the table it plots: care homes or non-residential."""
        mismatched = [
            (support.chart_title(chart), expected)
            for chart in support.charts(package, sheet.name)
            for expected in [
                support.expected_comparison_title(support.series_columns(chart))
            ]
            if support.chart_title(chart) != expected
        ]

        assert mismatched == [], "Chart titles that do not match: (found, expected)"

    def test_filled_posts_charts_plot_the_column_their_title_names(
        self, package: zipfile.ZipFile
    ):
        """'CQC care homes' plots care homes, and so on, not another service type."""
        plotted = {
            support.chart_title(chart): support.series_columns(chart)
            for chart in support.charts(package, cell_map.FILLED_POSTS.name)
        }

        assert plotted == FILLED_POSTS_CHART_COLUMNS

    def test_filled_posts_charts_plot_filled_posts_first_then_locations(
        self, package: zipfile.ZipFile
    ):
        """Series one is the filled posts block and series two the CQC locations block."""
        sheet = cell_map.FILLED_POSTS
        monthly_percent_notes = (
            "monthly filled posts % change",
            "monthly CQC locations % change",
        )
        expected_rows = [
            support.rows_of(f"'x'!{r.cells}")
            for r in sheet.input_ranges
            if r.note in monthly_percent_notes
        ]
        wrong_order = []
        for chart in support.charts(package, sheet.name):
            series = chart.findall(".//c:ser", NS)
            rows = [support.rows_of(s.find("c:val//c:f", NS).text) for s in series]
            names = [
                support.split_reference(s.find("c:tx//c:f", NS).text)[1] for s in series
            ]
            if (rows, names) != (expected_rows, list(sheet.chart_label_ranges)):
                wrong_order.append(support.chart_title(chart))

        assert wrong_order == [], "Charts whose series are in a different order"


class TestChartAppearance:
    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_charts_have_a_category_axis(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """The checks on the category axis below find an axis on every chart."""
        without_category_axis = [
            index
            for index, chart in enumerate(support.charts(package, sheet.name))
            if not chart.findall(".//c:catAx", NS)
        ]

        assert without_category_axis == []

    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_category_axes_are_automatic_so_unfilled_slots_take_no_space(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """An automatic date axis drops empty trailing slots instead of leaving a gap."""
        non_automatic = [
            index
            for index, chart in enumerate(support.charts(package, sheet.name))
            for axis in chart.findall(".//c:catAx", NS)
            if axis.find("c:auto", NS) is None
            or axis.find("c:auto", NS).get("val") != "1"
        ]

        assert non_automatic == [], "Charts with a fixed text category axis"

    @pytest.mark.parametrize("sheet", support.charted_sheet_params)
    def test_charts_have_no_per_point_label_overrides(
        self, sheet: cell_map.SheetMap, package: zipfile.ZipFile
    ):
        """openpyxl loses per-point labels on save, which would show every label."""
        overridden = [
            index
            for index, chart in enumerate(support.charts(package, sheet.name))
            if chart.findall(".//c:dLbl", NS)
        ]

        assert overridden == [], "Charts with per-point data label overrides"
