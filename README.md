# DataEngineering

## Welcome to the Skills for Care Data Engineering repository.

This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

Be sure to check out our [Wiki](https://github.com/NMDSdevopsServiceAdm/DataEngineering/wiki) for more info!

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


# Mission Statement

*"INSERT MISSION STATEMENT*

# Building the project

### Prerequisite installs:
Tool | Windows | Mac/Linux
--- | --- | ---
Python | https://www.python.org/downloads/ | https://www.python.org/downloads/
Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git
Pyenv | https://github.com/pyenv-win/pyenv-win | https://github.com/pyenv/pyenv
Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/ | https://pipenv-fork.readthedocs.io/en/latest/install.html
java jdk8 | https://www.java.com/en/download/ | https://www.java.com/en/download/

## Install Java (MacOS)
This project is using jdk8. We recommend using Brew (https://brew.sh) to install the java development kit. This project is using **jdk8**.
```
brew update
brew install adoptopenjdk8
```


## Clone the project
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
```
## Create virtual environment and install dependencies
```
cd DataEngineering
pipenv install --dev
```

For detailed Windows setup see here: https://github.com/NMDSdevopsServiceAdm/DataEngineering/blob/main/WindowsSetup.md


### Start virtual env
```
pipenv shell
```
### Stop virtual env
```
exit
```
#### IMPORTANT
Do not use `deactivate` or `source deactivate` - this will leave pipenv in a confused state because you will still be in that spawned shell instance but not in an activated virtualenv. 

## Testing
### Run test
*Make sure you have the virtual environment running (see above).*

Run specific test:
```
python -m unittest tests/unit/<test_name.py>
```
Run all tests
```
python -m unittest discover tests/unit "test_*.py"
```

For verbose output add `-v` to the end of the command.


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
We utilise AWS EMR (Elastic Map Reduce) for our notebook environment. Below are the steps required to get this environment running.

1. Head over to AWS [EMR](https://eu-west-2.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-2)
2. Select *"Clusters"* from the left navigation column.
3. If there isn't a cluster already running, clone a new one from the most recently terminated.
    - Select the most recently terminated cluster.
    - Select *"Clone"* from the top navigation
    - Confirm *"yes"* to *"...including steps..."*
    - Check the hardware configuration found in step 2 is appropriate. We usually utilise 1 m5.xlarge master node and 3 m5.xlarge core nodes. By default we utilise spot pricing for reduced operational costs.
    - Complete the wizzard by clicking *"Create Cluster"*
4. Wait for cluster to finishing building (2 - 10 minutes)
5. Navigate to *"Notebooks"* from the left navigation column.
6. Either create a new notebook, or start a pre-existing one.
7. Wait for notebok to start (1-3 minutes)
8. Select notebook and click *"Open in JupyterLab"* - This will start your interactive notebook session. 
9. Once finished with the notebooks terminate the cluster.
    - Navigate to *"Clusters"* from the left navigation column.
    - Select the running cluster
    - Click *"Terminate"*


## Notebook costs
An EMR cluster is charged per instance minute, for this reason ensure the cluster is terminated when not in use.
The notebooks are free, but require a cluster to run on. 
The AWS EMR costing documentation can be found here: https://aws.amazon.com/emr/pricing/
