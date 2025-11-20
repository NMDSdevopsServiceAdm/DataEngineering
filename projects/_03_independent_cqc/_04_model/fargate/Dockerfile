FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils /app/polars_utils
COPY projects/_03_independent_cqc/_04_model/utils /app/projects/_03_independent_cqc/_04_model/utils
COPY utils /app/utils

# Copy all python jobs to WORKDIR
COPY projects/_03_independent_cqc/_04_model/fargate/*.py .

COPY projects/_03_independent_cqc/_04_model/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
