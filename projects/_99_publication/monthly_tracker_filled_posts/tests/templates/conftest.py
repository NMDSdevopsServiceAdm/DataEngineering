from collections.abc import Iterator
import zipfile

import openpyxl
import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)


@pytest.fixture(scope="module")
def package() -> Iterator[zipfile.ZipFile]:
    """The template as a zip package, for reading its XML."""
    with zipfile.ZipFile(cell_map.TEMPLATE_PATH) as package:
        yield package


# Shared by many tests, and `worksheet.cell()` creates missing cells, so tests
# that need to tell a missing cell from a styled one reload the workbook.
@pytest.fixture(scope="module")
def workbook() -> openpyxl.Workbook:
    """The template loaded with openpyxl."""
    return openpyxl.load_workbook(cell_map.TEMPLATE_PATH)
