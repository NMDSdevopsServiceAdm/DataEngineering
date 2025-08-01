FROM python:3.12-slim

ENV POLARS_TEMP_DIR=/tmp/polars

RUN mkdir -p /tmp/polars

WORKDIR /app

ENV PYTHONPATH=/app

COPY polars_utils /app/polars_utils
COPY projects/_01_ingest/cqc_api/utils /app/projects/_01_ingest/cqc_api/utils
COPY schemas/cqc_provider_schema_polars.py /app/schemas/cqc_provider_schema_polars.py
COPY projects/_01_ingest/cqc_api/fargate/requirements.txt /app/requirements.txt
COPY projects/_01_ingest/cqc_api/fargate/delta_download_cqc_providers.py /app/delta_download_cqc_providers.py

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "delta_download_cqc_providers.py"]