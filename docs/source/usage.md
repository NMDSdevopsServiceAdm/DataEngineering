# Usage

For further info on the steps I took to get this far, check out the [Walkthrough on Confluence](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1028227086/Sphinx)
Read through the steps in the [Setting up](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1028227086/Sphinx#Setting-up) section to get started, and from there explore the specific sections of relevance.

## Autodoc our code
You could manually type a function description like this:

```{eval-rst}
.. function:: lumache.get_random_ingredients(kind=None)

Return a list of random ingredients
    :param kind: Optional "kind" argument
    :type kind: list[str] or None
    :returns: The ingredients list
    :rtype: list[str]
```

>But we want to keep our function descriptions as close to the code as possible, which we can do via docstrings.
It turns out there is a way to get sphinx to read this

Here is an example of one of our docstrings, from the `utils.utils.remove_already_cleaned_data(df, destination)` function.
Check out the code in this file to see the magic.

### How it looks to manually type a google format docstring in rst format

```{eval-rst}
.. function:: utils.utils.example_latest_datefield_for_grouping(df: DataFrame, grouping_column_list: list, date_field_column: Column)

    For a particular column of dates, filter the latest of that date for a select grouping of other columns, returning a full dataset.
    Note that if the provided date_field_column has multiple of the same entries for a grouping_column_list, then this function will return those duplicates.

    :Args:
        df: The DataFrame to be filtered
        grouping_column_list: A list of pyspark.sql.Column variables representing the columns you wish to groupby, i.e. [F.col("column_name")]
        date_field_column: A formatted pyspark.sql.Column of dates

    :Returns:
        latest_date_df: A dataframe with the latest value date_field_column only per grouping

    :Raises:
        TypeError: If any parameter other than the DataFrame does not contain a pyspark.sql.Column

```

### How it looks to automatically render it from source
```{eval-rst}
.. autofunction:: utils.utils.latest_datefield_for_grouping
```
