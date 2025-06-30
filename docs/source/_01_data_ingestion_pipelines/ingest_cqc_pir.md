# Ingest CQC PIR Pipeline

This Step Function defines the ingestion, cleaning, and validation process for the CQC Provider Information Return (PIR) dataset.

## Overview

This pipeline is automatically triggered by an [Amazon EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/what-is-amazon-eventbridge.html) rule when a new CSV is added to a specific location in the S3 bucket. This allows the pipeline to respond dynamically to new data uploads without manual intervention.

The pipeline consists of several stages:

1. **Ingest CQC PIR**: Fetch raw CQC PIR data from the S3 source (in CSV files) and store it in S3 (in Parquet files).
2. **Clean and validate**: The raw data is cleaned and validated in parallel:
    - **Clean CQC PIR Data**: Applies cleaning transformations and writes cleaned output to S3.
    - **Validate Ingested Data**: Runs validation checks on the raw data and generates a report.
3. **Validate Cleaned Data**: Compares raw and cleaned datasets, generating a second validation report.
4. **Crawler Runs**: AWS Glue crawlers update the Glue Data Catalog with the validation and cleaned data.
5. **Error Handling**: If any stage fails, the pipeline sends a notification and still triggers the crawlers to maintain catalog consistency.

## Structure

The Step Function is structured using a mix of **parallel** and **sequential** tasks:

- **Parallel Branch**: Cleans and validates the raw CQC PIR data simultaneously.
- **Catch Block**: Catches all errors and sends them to a Lambda function that raises a notification, before continuing with the crawler steps.

## Tasks

| Step | Description | AWS Resource |
|------|-------------|--------------|
| Ingest CQC PIR data | Ingests raw CQC PIR data | `glue:startJobRun.sync` |
| Clean CQC PIR data | Cleans the raw data | `glue:startJobRun.sync` |
| Validate ingested CQC PIR data | Validates the raw data | `glue:startJobRun.sync` |
| Validate cleaned CQC PIR data | Validates the cleaned data | `glue:startJobRun.sync` |
| Start validation crawler | Updates validation dataset catalog | `glue:startCrawler` |
| Start CQC crawler | Updates CQC dataset catalog | `glue:startCrawler` |
| Error notification | Publishes error using Lambda | `lambda:invoke.waitForTaskToken` |

## Error Handling

All errors in the main workflow are caught by a `Catch` block, which triggers a Lambda to send notifications. Even in failure, the validation and CQC crawlers are run to keep the catalog updated.

## Outputs

- Cleaned CQC PIR dataset in S3.
- Data validation reports for both raw and cleaned datasets.
- Glue Data Catalog updated with new partitions for downstream usage.

## Parameters

This pipeline uses several parameterised values, passed dynamically:

- `${ingest_cqc_pir_job_name}`
- `${dataset_bucket_uri}`
- `${clean_cqc_pir_data_job_name}`
- `${validate_pir_raw_data_job_name}`
- `${validate_pir_cleaned_data_job_name}`
- `${pipeline_failure_lambda_function_arn}`
- `${cqc_crawler_name}`
- `${data_validation_reports_crawler_name}`

These are typically resolved at runtime via the calling context or environment configuration.

## Visual Representation

```{mermaid}
graph TD
    A[Ingest CQC PIR Data] --> B{Clean and Validate Raw Data}
    B --> C[Clean CQC PIR Data]
    B --> D[Validate Ingested Data]
    C --> E[Validate Cleaned Data]
    D --> E
    E --> F{Success?}
    F -->|Yes| G[Run Validation Crawler]
    G --> H[Run CQC Crawler]
    H --> I[Succeed]
    F -->|No| J[Notify via Lambda]
    J --> K[Run Validation Crawler When Failed]
    K --> L[Run CQC Crawler When Failed]
    L --> M[Fail]
```
