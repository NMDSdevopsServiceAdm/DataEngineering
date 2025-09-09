# How to refresh the docker image on AWS 
If you have made a change to the model training scripts, or added a model to model_registry.py you will need to redeploy Docker.
To do this, start by authenticating with AWS.

Create an environment variable with the AWS Account ID (replacing the sample with your account id):
```commandline
export AWS_ACCOUNT_ID=12345678912
```

Authenticate with ECR using:
```commandline
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com
```

Build your image with:
```commandline
 docker build -t fargate/model-retrain -f projects/_03_independent_cqc/_05a_model/fargate/Dockerfile .
```

Tag your image so Fargate knows where to put it:
```commandline
docker tag fargate/model-retrain:latest ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/model-retrain:latest
```

Push the image to your repository:
```commandline
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/model-retrain:latest
```
