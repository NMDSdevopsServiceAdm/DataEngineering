from unittest.mock import MagicMock

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.workbook_helpers as job


class TestSetColumnWidths:
    def test_function_sets_each_leading_column_to_its_width(self):
        worksheet = MagicMock()

        job.set_column_widths(worksheet, [10, 20, 30])

        assert worksheet.set_column.call_args_list == [
            ((0, 0, 10),),
            ((1, 1, 20),),
            ((2, 2, 30),),
        ]
