FROM public.ecr.aws/lambda/python:3.11

# Install the function's dependencies using file requirements.txt
# from your project folder.
WORKDIR ${LAMBDA_TASK_ROOT}
RUN mkdir utils


COPY lambdas/check_dataset_equality/requirements.txt .
RUN  pip install -r requirements.txt

# Copy function code
COPY lambdas/check_dataset_equality/check_dataset_equality.py .
COPY utils/column_names utils/column_names/
COPY projects/tools projects/tools/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]