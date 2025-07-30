FROM public.ecr.aws/lambda/python:3.11

WORKDIR ${LAMBDA_TASK_ROOT}
RUN mkdir utils

# Install the function's dependencies using file requirements.txt
COPY lambdas/create_dataset_snapshot/requirements.txt .
RUN pip install -r requirements.txt

# Copy function code
COPY lambdas/create_dataset_snapshot/lambda_function.py .
COPY lambdas/utils/snapshots.py utils/
COPY utils/column_names utils/

RUN ls .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]