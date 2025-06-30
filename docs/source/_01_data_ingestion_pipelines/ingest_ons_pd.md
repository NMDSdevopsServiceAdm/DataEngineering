# Ingest ONS Postcode Directory pipeline

This Step Function defines the ingestion, cleaning, and validation process for the ONS Postcode Directory dataset.

## Overview

The pipeline consists of several stages:

1. **Ingest ONS Data**: Fetch raw ONS postcode directory data from the S3 source (in CSV files) and store it in S3 (in Parquet files).
2. **Clean and Validate**: The raw data is cleaned and validated in parallel:
    - **Clean ONS Data**: Applies cleaning transformations and writes cleaned output to S3.
    - **Validate Ingested Data**: Runs validation checks on the raw data and generates a report.
3. **Validate Cleaned Data**: Compares raw and cleaned datasets, generating a second validation report.
4. **Crawler Runs**: AWS Glue crawlers update the Glue Data Catalog with the validation and cleaned data.
5. **Error Handling**: If any stage fails, the pipeline sends a notification and still triggers the crawlers to maintain catalog consistency.

## Structure

The Step Function is structured using a mix of **parallel** and **sequential** tasks:

- **Parallel Branch**: Cleans and validates the raw ONS data simultaneously.
- **Catch Block**: Catches all errors and sends them to a Lambda function that raises a notification, before continuing with the crawler steps.

## Tasks

| Step | Description | AWS Resource |
|------|-------------|--------------|
| Ingest ONS data | Ingests raw postcode directory data | `glue:startJobRun.sync` |
| Clean ONS data | Cleans the raw data | `glue:startJobRun.sync` |
| Validate ingested ONS data | Validates the raw data | `glue:startJobRun.sync` |
| Validate cleaned ONS data | Validates cleaned vs raw data | `glue:startJobRun.sync` |
| Start validation crawler | Updates validation dataset catalog | `glue:startCrawler` |
| Start ONS crawler | Updates ONS dataset catalog | `glue:startCrawler` |
| Error notification | Publishes error using Lambda | `lambda:invoke.waitForTaskToken` |

## Error Handling

All errors in the main workflow are caught by a `Catch` block, which triggers a Lambda to send notifications. Even in failure, the validation and ONS crawlers are run to keep the catalog updated.

## Outputs

- Cleaned ONS postcode directory dataset in S3.
- Data validation reports for both raw and cleaned datasets.
- Glue Data Catalog updated with new partitions for downstream usage.

## Parameters

This pipeline uses several parameterised values, passed dynamically:

- `${ingest_ons_data_job_name}`
- `${dataset_bucket_uri}`
- `${clean_ons_data_job_name}`
- `${validate_postcode_directory_raw_data_job_name}`
- `${validate_postcode_directory_cleaned_data_job_name}`
- `${pipeline_failure_lambda_function_arn}`
- `${ons_crawler_name}`
- `${data_validation_reports_crawler_name}`

These are typically resolved at runtime via the calling context or environment configuration.

## Visual Representation

```{mermaid}
graph TD
    A[Ingest ONS data] --> B{Clean and Validate}
    B --> C[Clean ONS data]
    B --> D[Validate ingested data]
    B --> E[Validate cleaned data]
    E --> F[Run validation crawler]
    F --> G[Run ONS crawler]
