# Skills for Care Data Engineering

[![CircleCI](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

## Welcome
This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

Be sure to check out our [Wiki](https://github.com/NMDSdevopsServiceAdm/DataEngineering/wiki) for more info!

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


## Mission Statement


### About Us

We are Skills for Care’s Workforce Intelligence Team, the experts in adult social care workforce insight.

Skills for Care, as the leading source of adult social care workforce intelligence, helps to create a better-led, skilled and valued adult social care workforce. We provide practical tools and support to help adult social care organisations in England recruit, retain, develop and lead their workforce. We work with employers and related services to ensure dignity and respect are at the heart of service delivery.

We’re commissioned by the Department of Health and Social Care to collect data on adult social care providers and their workforce via the Adult Social Care Workforce Data Set (previously named National Minimum Data Set for Social Care). For over 15 years we’ve turned this data into intelligence and insight that's relied upon by the Government and across our sector.

### About the Data Engineering Project

Our data engineering project aims to convert our data modeling processes into reproducible analytical pipelines in AWS. These pipelines will be more accurate and efficient, allowing us to:
 - model our estimates using all the data available to us
 - provide more frequent updates to our estimates
 - use more complex modelling techniques, such as machine learning or AI modules
 - make our detailed methods available to anyone who wants to explore them


## Testing
### Run tests
*Make sure you have the virtual environment running (see above).*

Run a specific test file once
```
python -m unittest tests/unit/<test_name.py>
```
Watch a specific test file and auto rerun the tests when there are changes
```
pytest-watch -m unittest tests/unit/<test_name.py>
```

Run all tests once
```
python -m unittest discover tests/unit "test_*.py"
```
Watch all the tests and auto rerun the tests when there are any changes
```
pytest-watch
```
### Run specific test within test file
```
python -m unittest tests.unit.<glue_job_test_folder>.<test_class>.<specific_test>
```
example:
```
python -m unittest tests.unit.test_clean_ascwds_workplace_data.MainTests.test_main
```


For verbose output add `-v` to the end of the command.


<br>
<br>

## Documentation
The documentation for the work we do here should be located in one of two places, as detailed in the subsections below

### Docstrings
We have started to implement docstrings within our code, and they are to follow the [Google Docstring Styleguide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Currently the approach is to ensure all functions within jobs have a docstring, then expand that out to classes and modules as desired.
This relaxed approach is because of Sphinx in the section below, which has been setup to automatically and dynamically pick up docstrings from code within the low-level documentation, and thus over time will become more and more populated. This will allow us to control the growth of documentation and visualise it the way we want to.
There is a [discussion and comparison of different Docstring standards on our Confluence](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1033994246/Docstrings+StyleGuides+and+Resources)

### Sphinx
For low level, close-to-code documentation and technical pipeline components, you can easily launch our documentation server once you've cloned our repository
- `sphinx-autobuild docs/source/ docs/build/`
*Note that if you already have a copy of our repository you will need to update your pipenv file* - Try doing this with the following before running the above if you have any issues:
- `pipenv sync --dev`

#### Making changes to documentation
Since Sphinx is configured to understand Markdown (.md) files, you can find all of the documentation (excluding the main repo README) within the `docs` directory.
To understand more about how the Sphinx documentation was setup, consult our [Sphinx page on Confluence](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1028227086/Sphinx)

### Confluence
For high level project documentation, including content such as handovers, ADRs, signposting, private decision logs and guidance otherwise not available or suitable for our public repo, you can request access to our Confluence here: [Skills For Care Data Engineering Confluence Home](https://skillsforcare.atlassian.net/wiki/spaces/DE/overview?homepageId=1011220675)


### Linting Python code

We use [black](https://black.readthedocs.io/en/stable/) to lint our Python code.
Install black using pip
```
pip install black
```
To lint all the Python files, first ensure you're at the root of the repository, then run
```
black .
```

### Linting Terraform code

#Install Terraform following [the instructions below](#installing-terraform).

Ensure you are at the root of the repository, then run
```
terraform fmt -recursive
```
