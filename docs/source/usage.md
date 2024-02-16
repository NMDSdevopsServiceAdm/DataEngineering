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
.. function:: utils.utils.example_remove_already_cleaned_data(df, destination)

    Removes already cleaned data from an existing dataframe that contains an import_date column

    :Args:
        df: A pyspark.sql.DataFrame you are checking
        destination: The absolute path of the cleaned data source to be checked against

    :Returns: Return a filter of a dataframe with the latest data,
        and if there is no new data then an empty dataframe is returned.
        If a file read is not possible, or the latest cleaned data import data is None,
        then the original dataframe supplied is returned.

    :Raises: AnalysisException when import_date is not located within the dataframe as a column
```

### How it looks to automatically render it from source

```{eval-rst}
.. autofunction:: utils.utils.remove_already_cleaned_data
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