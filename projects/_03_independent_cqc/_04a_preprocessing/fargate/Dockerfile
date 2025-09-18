FROM python:3.11-slim

ENV POLARS_TEMP_DIR=/tmp/polars

RUN mkdir -p /tmp/polars

WORKDIR /app

ENV PYTHONPATH=/app

COPY polars_utils /app/polars_utils
COPY projects/_03_independent_cqc/_04a_preprocessing/fargate/preprocessing.py /app/preprocessing.py
COPY projects/_03_independent_cqc/_05_model/fargate/requirements.txt /app/requirements.txt


RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "preprocessing.py"]