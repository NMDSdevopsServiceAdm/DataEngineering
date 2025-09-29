FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY utils utils
COPY polars_utils polars_utils
COPY projects/_01_ingest/cqc_api/utils projects/_01_ingest/cqc_api/utils
COPY schemas/cqc_provider_schema_polars.py schemas/cqc_provider_schema_polars.py
COPY schemas/cqc_locations_schema_polars.py schemas/cqc_locations_schema_polars.py
COPY projects/_01_ingest/cqc_api/fargate/utils/*.py projects/_01_ingest/cqc_api/fargate/utils
COPY projects/_01_ingest/cqc_api/utils/postcode_replacement_dictionary.py projects/_01_ingest/cqc_api/utils/postcode_replacement_dictionary.py 

# Copy all python jobs to WORKDIR
COPY projects/_01_ingest/cqc_api/fargate/*.py .

COPY projects/_01_ingest/cqc_api/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]