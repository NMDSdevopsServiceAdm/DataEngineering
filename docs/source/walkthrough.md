# Walkthrough

## Setting up

[Started here](https://www.sphinx-doc.org/en/master/usage/installation.html) which involved installing sphinx via brew, but there are other windows instructions too
When I attempted this brew install, I was instructed to use pip to install sphinx-doc instead. This command doesn't work, so I'm doing this instead as instructed on that page. I made sure I was in our pipenv shell first before running:
```bash
pip install -U sphinx
sphinx-build --version
```

Next I followed the steps here: https://www.sphinx-doc.org/en/master/tutorial/getting-started.html
This is what options I selected:

```bash
sphinx-quickstart docs

You have two options for placing the build directory for Sphinx output.
Either, you use a directory "_build" within the root path, or you separate
"source" and "build" directories within the root path.
> Separate source and build directories (y/n) [n]: y

The project name will occur in several places in the built documentation.
> Project name: Skills for Care Data Engineering
> Author name(s): Skills for Care
> Project release []: 1.0.0

If the documents are to be written in a language other than English,
you can select a language here by its language code. Sphinx will then
translate text that it generates into that language.

For a list of supported codes, see
https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-language.
> Project language [en]:
```
This process created some files.
```
Creating file /Users/lukewilliams/git-repositories/sfc-repos/DataEngineering/docs/source/conf.py.
Creating file /Users/lukewilliams/git-repositories/sfc-repos/DataEngineering/docs/source/index.rst.
Creating file /Users/lukewilliams/git-repositories/sfc-repos/DataEngineering/docs/Makefile.
Creating file /Users/lukewilliams/git-repositories/sfc-repos/DataEngineering/docs/make.bat.
```

Then built it with the following:

`sphinx-build -M html docs/source/ docs/build/`

And from there opened up the index page. Now I need to get it to see the README we already have there.
As we will see in the {ref}`Observations` section, the build process creates a whole lot of files which will clutter the version control, so I added the build directory to the gitignore. Time will tell if this is sensible or not, perhaps we might need this directory present but empty in version control, or not, I'm not sure at this stage.

## Getting visibility on the existing README

Started here: [https://www.sphinx-doc.org/en/master/usage/markdown.html#configuration](Sphinx configuration page).

`pip install --upgrade myst-parser`

And added an `extensions = ["myst_parser"]` part to the conf.py file.

Note the documentation for this parser is here, [extending the regular markdown functionality to work with Sphinx](https://myst-parser.readthedocs.io/en/latest/syntax/optional.html)


Not sure if that's had an effect yet, but from here moving on to customising the page
UPDATE: when running the autobuilder later it looks like it did work without needing to mess about with settings.

https://www.sphinx-doc.org/en/master/tutorial/first-steps.html

Curious if its possible to start the entire project in markdown rather than rst. In this endeavour saw a video where the specific section below he runs a script to convert all rst files into markdown, and looking at the markdown file he generates we can convert our own:

TODO figure out how to reference an image, and get the one from my Obsidian notebook attachment. For now, the below link is the video you can get the similar conclusion from.

> https://www.youtube.com/watch?v=qRSb299awB0

### Configuring an autobuilder

During the video above he installed another pip package, sphinx-autobuild:
`pip install sphinx-autobuild`

> https://youtu.be/qRSb299awB0?feature=shared&t=1559

Which he then used by running:
`sphinx-autobuild docs/source/ docs/build/`

I tried this myself and by the looks of it now changes will update in real time. I took a video of this.

### The trick to seeing the README
Fortunately the [video](https://www.youtube.com/watch?v=qRSb299awB0) showed how to do this too. It makes use of directives, which I think are documented in the form of [colon fences](https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#code-fences-using-colons)

The video uses the triple backtick notation but the link above provides another extension to enable this support in markdown

With this, we can use the {include} directive and then just use the README that is 2 directories above, i.e.
Put the following in an {include} directive:
"../../README.md"
and colon fence "relative-images". I can't do it explicitely here, but you can see this specific example on the main page (which is the " {doc}`index` " page)


## Customising Sphinx a bit more
### sphinx duration
This extension shows how long it takes to render
`"sphinx.ext.duration"`
TODO can we get the screenshot here? For now, here is a note that this time appears in the console whenever you make a change and it re-builds the page

### Allow links to subheadings
You need another internal extension, `"sphinx.ext.autosectionlabel"`

This allows you to reference a subheading by doing something like the following:

{ref}`Allow links to subheadings`

### More modern theme
`html_theme = "furo"`

`pip install furo`

Added the furo version to the dev dependencies in pipfile too.
The theme is more modern than the default, and looks way better. It also allows for dark/light screen switching for one thing, and you can find more of it's features on the [furo github page](https://github.com/pradyunsg/furo).

More themes in their [Themes Gallery](https://sphinx-themes.org/)

## Automatically generating documentation from code

Starting from [this point in the video](https://youtu.be/qRSb299awB0?feature=shared&t=2996) he starts to discuss this.

Sphinx has tools for documenting code objects in multiple domains, and for our use case which is python, there is a [domain for python](https://www.sphinx-doc.org/en/master/usage/domains/python.html)

### Steps to automatic
There are a few steps you need to do, according to the guides:
1. enable the `"sphinx.ext.autodoc"` extension in conf.py
2. Simply reference the code function you want to document
>This didn't quite work for me since my function was nested... I get an error during rendering which says utils can't be found. To fix this error I also needed to
3. extend the path of the conf.py file - more detail on this in my scattered thoughts below.

### Scattered thoughts on this process
Need to play about with what a nested python file looks like when referenced to figure out this error after step 2
The documentation on [auto generation from code](https://www.sphinx-doc.org/en/master/tutorial/automatic-doc-generation.html) might help, but ultimately something is missing...

Further reading seems to indicate that there is something needed in the conf.py file, imports and paths to specify where the modules are.

This page shines some light: https://www.sphinx-doc.org/en/master/usage/configuration.html
Can extend the conf.py file with system paths. 

>[!yes] Cracked it! You need to extend the path of the conf.py file
I found the [solution on stack overflow](https://stackoverflow.com/questions/10324393/sphinx-build-fail-autodoc-cant-import-find-module?rq=4)


You can do this quite simply by inserting the following (since we split build and source path) into conf.py:
```
import sys, os

sys.path.insert(0, os.path.abspath("../.."))
```

To see this in action, go and checkout the {doc}`usage` page, as it's in its own section for ease of viewability in this POC

## Observations

### Version control noise in the build/ directory
There is a lot of noise in version control, so will probably need to add some content to the gitignore not to commit any of the content we aren't actively making changes too. This content should render locally or in the pipeline but not reach version control.
![[Screenshot 2024-02-15 at 18.34.02.png]]

Most of this is build content, so adding this to the .gitignore was enough:
```
# ------ sphinx artefacts ---- #
docs/build
```

### Rendering new code / docstring changes
Sphinx picks up on changes in the documentation when autobuilding, NOT code or docstrings. To force it to update this content, simply make a change on a document page and save it. That will make the autobuilder reconstruct everything rather than just check to see if documentation changed

### Automodule - potential further research
There were a few stack overflow articles and sphinx tutorials I came across, such as https://stackoverflow.com/questions/2701998/automatically-document-all-modules-recursively-with-sphinx-autodoc, which mention automatically documenting all modules recursively, to save on manual definition of one function at a time... if that's something we wanted to consider as we build docs. Although I currently feel this would work best if the scripts were already configured to docstring standards and didn't need to be desribed in how they interact.

### Doctests
Not really looked into it, but there might be a way to test documentation with tests too. I don't really see the use case for this if our unittests are working as expected, and is probably more for python projects without a test framework.

https://www.sphinx-doc.org/en/master/tutorial/describing-code.html#including-doctests-in-your-documentation

Reading into this a bit further, implies that functional examples within docstrings of code can be tested using doctests, so for example
```
def my_function(a, b):
    """
    >>> my_function(2, 3)
    6
    >>> my_function('a', 3)
    'aaa'
    """
    return a * b
```
You can use the ">>>" notation within the docstring and doctest would pick up that this is a run of the function, and would attempt it. It would generate a failure if the output does not match the next line in the docstring.
i.e. It would run 2 tests in the example above:

my_function(2, 3) == 6

my_function('a', 3) == 'aaa'