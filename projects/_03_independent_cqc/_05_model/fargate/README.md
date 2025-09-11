# How to refresh the docker image on AWS 
If you have made a change to the model training scripts, or added a model to model_registry.py you will need to redeploy Docker.
To do this, you need to ensure that you have [Docker](https://docs.docker.com/engine/install/) installed on your local
machine, including the `docker` command line utility. You will also need the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
and be able to authenticate to the AWS account.

Begin by authenticating to the AWS account.

Create an environment variable with the AWS Account ID (replacing the sample with your account id):
```commandline
export AWS_ACCOUNT_ID=12345678912
```
(You can see the account number in the top-right corner of the AWS console, after you log in, or you can use the
command `aws sts get-caller-identity` to retrieve it.)

Authenticate with ECR using:
```commandline
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com
```

Build your image with:
```commandline
 docker buildx build --platform linux/amd64 -f projects/_03_independent_cqc/_05_model/fargate/Dockerfile -t fargate/model-retrain . --load
```
(This command ensures that the Docker image created is compatible with the Linux operating system used in AWS Fargate.)

Tag your image so Fargate knows where to put it:
```commandline
docker tag fargate/model-retrain:latest ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/model-retrain:latest
```

Push the image to your repository:
```commandline
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/model-retrain:latest
```
