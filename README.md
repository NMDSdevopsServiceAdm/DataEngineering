# Skills for Care Data Engineering

[![CircleCI](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

## Welcome
This repository contains:
- Spark/Polars jobs for data transformation and feature extraction
- Terraform code for Amazon Web Services deployment

## About

### Who We Are
We are the Workforce Intelligence Team at Skills for Care, the experts in adult social care workforce data and insight in England.

Skills for Care, as the leading source of adult social care workforce intelligence, helps create a better-led, skilled and valued adult social care workforce. We provide practical tools and support to help adult social care organisations in England recruit, retain, develop and lead their workforce. We work with employers and related services to ensure dignity and respect are at the heart of service delivery.

We’re commissioned by the Department of Health and Social Care (DHSC) to collect data on adult social care providers and their workforce via the Adult Social Care Workforce Data Set (ASC-WDS) (previously named National Minimum Data Set for Social Care (NMDS-SC)). For over 15 years we’ve turned this data into intelligence and insight that's relied upon by the Government and across our sector.

### What This Project Does
This project builds reproducible data pipelines in Amazon Web Services to:
- Improve modelling accuracy and frequency
- Use more complex modelling techniques than previously possible
- Make our estimation methods transparent and repeatable

## Project Structure
```text
.
├── projects/               # Spark jobs, transformations and unit tests
├── terraform/              # Infrastructure as code
```

## Quickstart
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
cd DataEngineering
pipenv install --dev
pipenv shell
```

## Testing
```
# Run all tests
python -m unittest discover -s . -p "test_*.py"

# Watch tests
pytest-watch
```

## Linting
```
black .                   # Python
pydoclint .               # Docstring
terraform fmt -recursive  # Terraform
```

## Documentation
- Follow [Google style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- High-level documentation is maintained internally.
- For collaboration or more information, contact us at [analysis@skillsforcare.org.uk](mailto:analysis@skillsforcare.org.uk)

## Further Reading
- [CHANGELOG.md](./CHANGELOG.md) - notable changes to this project over time
- [DEPLOY.md](./DEPLOY.md) - Terraform deployment guide
- [GUIDES.md](./GUIDES.md) - general guidance and information
- [LICENSE.md](./LICENSE.md) - license information
- [NOTEBOOKS.md](./NOTEBOOKS.md) - Jupyter notebooks on Amazon Web Services EMR
- [SETUP.md](./SETUP.md) - local environment and project setup
- [STYLEGUIDE.md](./STYLEGUIDE.md) — naming, test writing, and docstring conventions
- [WINDOWSSETUP.md](./WINDOWSSETUP.md) — instructions for setting up the project on Windows

## Publications and Outputs
The data and models produced by this repository feed into various reports, dashboards and monthly trackers and are available at:
[Skills for Care data and publications](https://www.skillsforcare.org.uk/Adult-Social-Care-Workforce-Data/Workforce-intelligence/publications/Data-and-publications.aspx)

## License
This repository is published for transparency and public interest only. It is **not licensed for reuse**.

## Contact
For questions, suggestions, feedback, or if you wish to reuse or adapt this code, please contact [analysis@skillsforcare.org.uk](mailto:analysis@skillsforcare.org.uk)
