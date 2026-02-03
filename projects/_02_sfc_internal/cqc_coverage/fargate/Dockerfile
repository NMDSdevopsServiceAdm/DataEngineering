FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY utils/value_labels /app/utils/value_labels

# Copy python jobs to WORKDIR
COPY projects/_02_sfc_internal/cqc_coverage/fargate/*.py .

COPY projects/_02_sfc_internal/cqc_coverage/fargate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
