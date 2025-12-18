FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY projects/_03_independet_cqc/_01_merge/fargate/merge_utils projects/_03_independet_cqc/_01_merge/fargate/merge_utils
# COPY schemas/cqc_locations_schema_polars.py schemas/cqc_locations_schema_polars.py
# COPY schemas/cqc_provider_schema_polars.py schemas/cqc_provider_schema_polars.py
COPY utils utils

# Copy all python jobs to WORKDIR
COPY projects/_03_independet_cqc/_01_merge/fargate/*.py .

COPY projects/_03_independet_cqc/_01_merge/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
