FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY projects/_01_ingest/cqc_api/fargate/utils projects/_01_ingest/cqc_api/fargate/utils
COPY projects/_01_ingest/cqc_api/utils/cqc_api.py projects/_01_ingest/cqc_api/utils/cqc_api.py
COPY projects/_01_ingest/cqc_api/utils/validate_cqc_locations.py projects/_01_ingest/cqc_api/utils/validate_cqc_locations.py
COPY schemas/cqc_locations_schema_polars.py schemas/cqc_locations_schema_polars.py
COPY schemas/cqc_provider_schema_polars.py schemas/cqc_provider_schema_polars.py
COPY utils/column_names utils/column_names
COPY utils/column_values utils/column_values
COPY utils/value_labels utils/value_labels
COPY utils/aws_secrets_manager_utilities.py utils/aws_secrets_manager_utilities.py
COPY utils/cqc_local_authority_provider_ids.py utils/cqc_local_authority_provider_ids.py
COPY utils/s3_file_utils.py utils/s3_file_utils.py

# Copy all python jobs to WORKDIR
COPY projects/_01_ingest/cqc_api/fargate/*.py .

COPY projects/_01_ingest/cqc_api/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
