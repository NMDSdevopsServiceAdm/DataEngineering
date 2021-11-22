# DataEngineering

## Welcome to the Skills for Care Data Engineering repository.

This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


# Building the project
First install pipenv, the python environment manager.
```
brew install pipenv
```

Clone the project
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
```
Create virtual environment and install dependencies
```
cd DataEngineering
pipenv install --dev
```

<br>
<br>

# Deploying the Pipeline

Terraform docs can be found here:  https://www.terraform.io/docs/cli/run/index.html

1. Ensure you set the following environment variables

``` 
export TF_VAR_aws_secret_key= [ aws secret key ]
export TF_VAR_aws_access_key= [ aws access key ]
```

2. From terminal/command line ensure you're in the root directory of the project

```
adamprobert@Adams-MBP DataEngineering % pwd
/Users/adamprobert/Projects/skillsforcare/DataEngineering
```

3. Run `terraform plan` to evaluate the planned changes
```
terraform plan
```

4. Check the planned changes to make sure they are correct
5. Apply the terraform plan and confirm with `yes` when prompted
```
terraform apply
```