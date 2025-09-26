FROM python:3.11-slim

ENV POLARS_TEMP_DIR=/tmp/polars

RUN mkdir -p /tmp/polars

WORKDIR /app

ENV PYTHONPATH=/app

COPY utils /app/utils
COPY polars_utils /app/polars_utils
COPY projects/_03_independent_cqc/_05_model/fargate/retraining/train_model.py /app/train_model.py
COPY projects/_03_independent_cqc/_05_model/model_registry.py /app/projects/_03_independent_cqc/_05_model/model_registry.py
COPY projects/_03_independent_cqc/_05_model/utils/model.py /app/projects/_03_independent_cqc/_05_model/utils/model.py
COPY projects/_03_independent_cqc/_05_model/utils/version_manager.py /app/projects/_03_independent_cqc/_05_model/utils/version_manager.py
COPY projects/_03_independent_cqc/_05_model/fargate/retraining/requirements.txt /app/requirements.txt


RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]