# DataEngineering

## Welcome to the Skills for Care Data Engineering repository.

This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


# Building the project

### Prerequisite installs:
Tool | Windows | IOS
--- | --- | ---
Python | https://www.python.org/downloads/ | https://www.python.org/downloads/
Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git
Pyenv | https://github.com/pyenv-win/pyenv-win | https://github.com/pyenv/pyenv
Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/ | https://pipenv-fork.readthedocs.io/en/latest/install.html


### Clone the project
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
```
### Create virtual environment and install dependencies
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

# Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

----

## Spinning up a notebook

1. Head over to [AWS Glue](https://eu-west-2.console.aws.amazon.com/glue/home?region=eu-west-2)
2. Click *Notebooks* under *Dev endpoints* on the left navigation column.
3. Select *Create notebook*
4. Enter a *Notebook name*
5. Attach to develepment endpoint: "data-engineering-dev-endpoint"
6. Select pre-existing IAM role: "sm-notebook-iam-role-data-engineering"
7. Select *Create notebook* - these take 5~ minutes to initialise
8. Once the notebook Status = *Ready*, you can open the notebook and begin working.
