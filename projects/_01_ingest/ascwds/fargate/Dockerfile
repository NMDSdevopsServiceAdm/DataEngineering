FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY projects/_01_ingest/ascwds/fargate/utils projects/_01_ingest/ascwds/fargate/utils
COPY utils/column_names utils/column_names
COPY utils/column_values utils/column_values

# Copy all python jobs to WORKDIR
COPY projects/_01_ingest/ascwds/fargate/*.py .

COPY projects/_01_ingest/ascwds/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
