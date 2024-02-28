# Usage

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


## Setting a docstring convention.
### Resources
As hinted in the manual examples, there are better ways to structure Docstrings.

There are a few articles that I've had a look at:

- https://betterprogramming.pub/the-guide-to-python-docstrings-3d40340e824b
- https://docs.python-guide.org/writing/documentation/
- https://www.dataquest.io/blog/documenting-in-python-with-docstrings/

But the specific PEP guidelines around comments and Docstrings are in these resources:

- https://peps.python.org/pep-0008/#comments
- https://peps.python.org/pep-0257/#specification

### What standards exist
The [PEP docstring standards](https://peps.python.org/pep-0257/#specification), in it's simplest sense, indicate 2 types of doc strings:
1. [Single Line Docstrings](https://peps.python.org/pep-0257/#one-line-docstrings)
2. [Multi Line Docstrings](https://peps.python.org/pep-0257/#multi-line-docstrings)

Which you are free to read at your leisure, but I think it's more beneficial to focus on the four primary types of docstrings, all of which follow the above recommendations:

- [NumPy/SciPy docstrings](https://numpydoc.readthedocs.io/en/latest/format.html)
- [Google docstrings](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings)
- [reStructuredText](https://docutils.sourceforge.io/rst.html)
- [Epytext](https://epydoc.sourceforge.net/epytext.html)

Looking through these, I feel like Epytext is a good resource for understanding what characters can be present in Docstrings, but ultimately the Google docstrings example is most closely aligned with my own view on docstrings. Some core components:

- It agrees with me on not requiring docstrings for unit tests
- Allows one line docstrings, but encourages more
- Providers a template which should work with sphinx, as shown in the above example.