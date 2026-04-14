FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY projects/_01_ingest/cqc_api/fargate/utils projects/_01_ingest/cqc_api/fargate/utils
COPY projects/_01_ingest/cqc_api/utils projects/_01_ingest/cqc_api/utils
COPY schemas/cqc_locations_schema_polars.py schemas/cqc_locations_schema_polars.py
COPY schemas/cqc_provider_schema_polars.py schemas/cqc_provider_schema_polars.py
COPY utils utils

# Copy all python jobs to WORKDIR
COPY projects/_01_ingest/cqc_api/fargate/*.py .

COPY projects/_01_ingest/cqc_api/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]