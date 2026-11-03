import datetime

import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_sheet_text as job,
)

APRIL_2024 = datetime.date(2024, 4, 1)
APRIL_2025 = datetime.date(2025, 4, 1)
APRIL_2026 = datetime.date(2026, 4, 1)
TERM_FROM_DATES = job.TermFromDates(
    short=APRIL_2026, medium=APRIL_2025, long=APRIL_2024
)
RUN_NUMBER = 7


class TestTitleText:
    def test_uses_the_latest_period_by_date_not_by_text(self):
        title = job.title_text(["Sep 2025", "Mar 2026"], RUN_NUMBER)

        assert title.startswith("End of March 2026")

    def test_appends_the_run_number_to_the_latest_period(self):
        assert job.title_text(["Mar 2026"], RUN_NUMBER) == "End of March 2026 (run 7)"

    def test_raises_when_there_are_no_period_labels(self):
        with pytest.raises(ValueError, match="period_labels is empty"):
            job.title_text([], RUN_NUMBER)


class TestCompPrevRows:
    def test_lists_march_periods_then_periods_after_the_latest_march(self):
        months = [
            datetime.date(2026, 5, 1),
            datetime.date(2025, 9, 1),
            datetime.date(2025, 3, 1),
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
            datetime.date(2026, 5, 1),
        ]

        rows = job.comp_prev_rows(months)

        assert rows == [
            (datetime.date(2025, 3, 1), "Annual"),
            (datetime.date(2026, 3, 1), "Annual"),
            (datetime.date(2026, 4, 1), "Monthly"),
            (datetime.date(2026, 5, 1), "Monthly"),
        ]

    def test_raises_when_no_period_is_a_march(self):
        with pytest.raises(ValueError, match="no March"):
            job.comp_prev_rows([datetime.date(2026, 4, 1)])


class TestSheetText:
    PERIOD_LABELS = ["Mar 2025", "Sep 2025", "Mar 2026", "Sep 2026"]

    def test_titles_every_sheet_with_the_run_number(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)

        titles = {name: cells[cell_map.TITLE_CELL] for name, cells in text.items()}
        assert titles == {
            sheet.name: "End of September 2026 (run 7)"
            for sheet in cell_map.ASSESSMENT_SHEETS
        }

    def test_short_term_heading_and_note_use_the_march_before_the_term_start(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.COMP_SHORT_TERM.name
        ]

        assert text[cell_map.HEADING_CELL] == (
            "Comparison of Skills for Care's estimate of filled posts to Capacity"
            " Tracker - Since March 2026"
        )
        assert text[cell_map.CRITERIA_NOTE_CELL] == (
            "- Must have a Capacity Tracker figure in all months from end of"
            " March 2026 onwards."
        )

    def test_headings_follow_the_term_start_dates_not_the_period_labels(self):
        term_from_dates = job.TermFromDates(
            short=datetime.date(2027, 4, 1),
            medium=datetime.date(2026, 4, 1),
            long=APRIL_2024,
        )

        text = job.sheet_text(self.PERIOD_LABELS, term_from_dates, RUN_NUMBER)

        assert text[cell_map.COMP_SHORT_TERM.name][cell_map.HEADING_CELL].endswith(
            "Since March 2027"
        )
        assert text[cell_map.COMP_MEDIUM_TERM.name][cell_map.HEADING_CELL].endswith(
            "Since March 2026"
        )
        assert text[cell_map.COMP_LONG_TERM.name][cell_map.HEADING_CELL].endswith(
            "Since March 2024"
        )
        assert text[cell_map.FILLED_POSTS.name][
            cell_map.FILLED_POSTS_CHART_HEADING_CELL
        ].endswith("since March 2027")

    def test_medium_term_heading_and_note_use_the_march_before(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.COMP_MEDIUM_TERM.name
        ]

        assert text[cell_map.HEADING_CELL].endswith("Since March 2025")
        assert text[cell_map.CRITERIA_NOTE_CELL].endswith("end of March 2025 onwards.")

    def test_filled_posts_chart_heading_uses_the_march_before_the_short_term_start(
        self,
    ):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.FILLED_POSTS.name
        ]

        assert text[cell_map.FILLED_POSTS_CHART_HEADING_CELL] == (
            "Comparison of percentage change in filled posts and CQC locations"
            " since March 2026"
        )

    def test_long_term_heading_and_note_use_the_march_before(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.COMP_LONG_TERM.name
        ]

        assert text[cell_map.HEADING_CELL].endswith("Since March 2024")
        assert text[cell_map.CRITERIA_NOTE_CELL].endswith("end of March 2024 onwards.")

    def test_no_filters_sheet_gets_the_long_term_heading_but_no_note(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.NO_FILTERS.name
        ]

        assert text[cell_map.HEADING_CELL].endswith("Since March 2024")
        assert cell_map.CRITERIA_NOTE_CELL not in text

    def test_only_comparison_sheets_get_a_note(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES, RUN_NUMBER)

        with_note = {
            name for name, cells in text.items() if cell_map.CRITERIA_NOTE_CELL in cells
        }
        assert with_note == {
            cell_map.COMP_SHORT_TERM.name,
            cell_map.COMP_MEDIUM_TERM.name,
            cell_map.COMP_LONG_TERM.name,
        }

    def test_raises_when_no_period_label_is_a_march(self):
        with pytest.raises(ValueError, match="no March"):
            job.sheet_text(["Sep 2025", "Oct 2025"], TERM_FROM_DATES, RUN_NUMBER)

    def test_comp_prev_publication_labels_are_annual_rows_then_monthly_rows(self):
        # Sep 2025 is between the Marches, so it is not a row; May 2026 is repeated.
        period_labels = [
            "Mar 2025",
            "Sep 2025",
            "Mar 2026",
            "Apr 2026",
            "May 2026",
            "Jun 2026",
            "May 2026",
        ]

        text = job.sheet_text(period_labels, TERM_FROM_DATES, RUN_NUMBER)[
            cell_map.COMP_PREV_PUBLICATION.name
        ]

        labels = {cell: value for cell, value in text.items() if cell != "B3"}
        expected_rows = ["Annual", "Annual", "Monthly", "Monthly", "Monthly"]
        assert labels == {
            f"B{first_row + i}": label
            for first_row in (23, 64, 106)
            for i, label in enumerate(expected_rows)
        }

    def test_raises_when_periods_exceed_the_label_slots(self):
        # 21 March periods and 17 months after the latest March.
        labels = [f"Mar {year}" for year in range(2000, 2021)]
        labels += [f"{month} 2020" for month in ("Apr", "May", "Jun", "Jul", "Aug")]
        labels += [f"{month} 2020" for month in ("Sep", "Oct", "Nov", "Dec")]
        labels += [f"{month} 2021" for month in ("Jan", "Feb", "Apr", "May")]
        labels += [f"{month} 2021" for month in ("Jun", "Jul", "Aug", "Sep")]

        with pytest.raises(ValueError, match="exceed the 37 rows"):
            job.sheet_text(labels, TERM_FROM_DATES, RUN_NUMBER)
