"""Helpers shared by the tests that check how a workbook opens."""

import openpyxl


def selected_tab_titles(workbook: openpyxl.Workbook) -> list[str]:
    """Titles of the tabs that are selected; more than one groups the tabs."""
    return [sheet.title for sheet in workbook if sheet.sheet_view.tabSelected]


def sheets_not_at_a1(workbook: openpyxl.Workbook) -> list[str]:
    """Titles of the sheets that open scrolled down or without A1 selected."""
    return [
        sheet.title
        for sheet in workbook
        if sheet.sheet_view.topLeftCell not in (None, "A1")
        or any(
            (selection.activeCell, str(selection.sqref)) != ("A1", "A1")
            for selection in sheet.sheet_view.selection
        )
    ]
