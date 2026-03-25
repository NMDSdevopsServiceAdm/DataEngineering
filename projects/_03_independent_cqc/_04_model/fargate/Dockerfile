FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils /app/polars_utils
COPY projects/_03_independent_cqc/_04_model/utils /app/projects/_03_independent_cqc/_04_model/utils
COPY projects/_03_independent_cqc/_04_model/registry/model_registry.py /app/projects/_03_independent_cqc/_04_model/registry/model_registry.py
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/

# Copy all python jobs to WORKDIR
COPY projects/_03_independent_cqc/_04_model/fargate/*.py .

COPY projects/_03_independent_cqc/_04_model/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
