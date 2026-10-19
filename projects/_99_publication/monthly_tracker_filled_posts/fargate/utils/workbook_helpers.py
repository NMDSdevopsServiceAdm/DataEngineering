def set_column_widths(worksheet: object, widths: list[int]) -> None:
    """
    Sets the width of a worksheet's leading columns.

    Args:
        worksheet (object): the worksheet to size.
        widths (list[int]): width of each column from the first, in characters.
    """
    for column, width in enumerate(widths):
        worksheet.set_column(column, column, width)
