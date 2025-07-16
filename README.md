# Skills for Care Data Engineering

[![CircleCI](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

## Welcome
This repository contains:
- Spark jobs for data transformation and feature extraction
- Terraform code for AWS deployment

Be sure to check out our [Wiki](https://github.com/NMDSdevopsServiceAdm/DataEngineering/wiki) for more info!

## About

### Who We Are
We are the Workforce Intelligence Team at Skills for Care, the experts in adult social care workforce data and insight in England.

Skills for Care, as the leading source of adult social care workforce intelligence, helps create a better-led, skilled and valued adult social care workforce. We provide practical tools and support to help adult social care organisations in England recruit, retain, develop and lead their workforce. We work with employers and related services to ensure dignity and respect are at the heart of service delivery.

We’re commissioned by the Department of Health and Social Care (DHSC) to collect data on adult social care providers and their workforce via the Adult Social Care Workforce Data Set (ASC-WDS) (previously named National Minimum Data Set for Social Care (NMDS-SC)). For over 15 years we’ve turned this data into intelligence and insight that's relied upon by the Government and across our sector.

### What This Project Does
This project builds reproducible data pipelines in AWS to:
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
terraform fmt -recursive  # Terraform
```

## Documentation
- Follow [Google style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- High-level documentation is maintained internally.
- For collaboration or more information, contact us at [analysis@skillsforcare.org.uk](mailto:analysis@skillsforcare.org.uk)

## Further Reading
- [SETUP.md](./SETUP.md)
- [DEPLOY.md](./DEPLOY.md)
- [NOTEBOOKS.md](./NOTEBOOKS.md)
- [GUIDES.md](./GUIDES.md)
- [CHANGELOG.md](./CHANGELOG.md)

## Contact
For questions, suggestions or feedback, please contact:
📧 [analysis@skillsforcare.org.uk](mailto:analysis@skillsforcare.org.uk)

## License
This repository is published for transparency and public interest only.
It is **not licensed for reuse**. Please contact [analysis@skillsforcare.org.uk](mailto:analysis@skillsforcare.org.uk) if you wish to reuse or adapt this code.
