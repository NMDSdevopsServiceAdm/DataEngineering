# Ingest ONS Postcode Directory pipeline

This Step Function defines the ingestion, cleaning, and validation process for the ONS Postcode Directory dataset.

## Overview

This pipeline is automatically triggered by an [Amazon EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/what-is-amazon-eventbridge.html) rule when a new CSV is added to a specific location in the S3 bucket. This allows the pipeline to respond dynamically to new data uploads without manual intervention.

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
| Validate cleaned ONS data | Validates the cleaned data | `glue:startJobRun.sync` |
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
    A[Ingest ONS Data]:::format --> B{Clean and Validate}:::format
    B --> C[Clean ONS Data]:::format
    B --> D[Validate Ingested Data]:::format
    C --> E[Validate Cleaned Data]:::format
    D --> E
    E --> F{Success?}:::format
    F -->|Yes| G[Run Validation Crawler]:::format
    G --> H[Run ONS Crawler]:::format
    H --> I[Succeed]:::format
    F -->|No| J[Notify via Lambda]:::format
    J --> K[Run Validation Crawler When Failed]:::format
    K --> L[Run ONS Crawler When Failed]:::format
    L --> M[Fail]:::format

    classDef format fill:#F5FAFD,stroke:#005EB8,stroke-width:2px;
```
