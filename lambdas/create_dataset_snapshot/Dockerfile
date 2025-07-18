FROM public.ecr.aws/lambda/python:3.11

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN dir -s
WORKDIR /sfc_lambda

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target .

# Copy function code
COPY lambda_function.py .

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]