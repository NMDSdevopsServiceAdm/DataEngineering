# Changelog

All notable changes to this project will be documented in this file.


## [Unreleased]

### Added
- Added a lambda function to check if two datasets are equal

- Added a lambda function to create a full snapshot from a delta dataset


## [v2025.05.0] - 18/06/2025
This version marks the codebase used for the publication of the Size and Structure 2025 report.

### Added
- Included related_location as a new feature in the non-residential with and without dormancy models to better distinguish genuinely new services from previously registered ones.

- Added a pre-commit hook to enforce linting, docstring standards, and to block accidental commits of .show() statements.

- Developed dataset-specific Step Functions triggered on new S3 file uploads. Each runs ingestion, validation, and cleaning in sequence for more timely error detection and fresher data availability.

- Incorporated more PIR data into the non-residential dataset.

- A lower level of service breakdowns to include all the categories we group to in our publications.

### Changed
- Reorganised file structure: introduced a projects-based layout with scoped jobs/, tests/, and utils/ directories. This improves navigation and code ownership across datasets and processes.

- Retrained the linear regression model from v2.0.0 to v2.0.1 to include more recent data.

### Improved
- Switched the care home, non-residential without dormancy, and non-residential with dormancy models from Gradient Boosted Trees (GBT) to linear regression. The GBT models were overfitting and unstable at location level; the new models offer better explainability and more stable trends.

- Replaced the rate of change feature in our models with a rolling average trendline to reduce bias towards open locations and better reflect real trends, especially closures and new openings.

- Revised our interpolation approach to only interpolate across gaps of up to 6 months. Previously, longer gaps caused trends to flatten unnaturally between known values.

- Replaced the binary is_dormant feature with a continuous time_since_dormant metric in the non-residential with dormancy model, improving prediction smoothness for post-dormancy growth.

- Reduced rolling periods in trend and model features from 6 months to 3 months, allowing the trends to respond more quickly to genuine shifts in the data.

- Refined postcode matching logic. The new multi-step approach attempts: exact match, historical match, mapped replacement, and truncated match before failing, improving match rates and reducing pipeline failures.

- Enhanced filtering of grouped ASCWDS submissions where providers may be submitting their entire workforce into only one of their locations. These are now identified and the larger than expected values are nulled to prevent over-exaggerating the size of the workforce.


## [v2025.03.0] - 10/04/2025
Initial tagged release of the codebase.

This version marks the start of formal versioning and release tracking.
All previous work prior to this was unversioned.

### Added
- All code created up until this point in time
