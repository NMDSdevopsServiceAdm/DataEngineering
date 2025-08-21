# Use Cases
Tool | Use Case
--- | ---
S3 | Storing Data
Glue | Job automation, extract, transofrm and load (ETL), Scheduling, Metadata
Athena | Adhoc querying and investigation using structured query language (SQL)
Jupyter | Pre-production environment, useful for investigation
VS Code | Production/development environment, develop notebooks into automated, tested and documented jobs.
Circleci | Devops/infrastructure - automated testing and deployment


# Glossary of Tools

# Amazon Web Services
## S3
Amazon Simple Storage Service (Amazon S3) allows cloud storage for data objects.
Data objects are held within buckets (these are similar to folders within microsoft file explorer).
You can store as much data as you want within S3 and Amazon automatically backs up the data –
we have enabled [Bucket Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) on our S3 buckets so you can automatically recover something if it is accidentally deleted.
Administrators can control who can set permissions for who can access each bucket.

## Glue
>Amazon Web Services Glue is a fully managed extract, tranform and load (ETL) service.
>Using Glue, you can categorize your data, clean it, enrich it, and move it between various data stores and data streams.
>Amazon Web Services Glue consists of a central metadata repository known as the Amazon Web Services Glue Data Catalog, an extract, tranform and load engine that automatically generates Python or Scala code, and a scheduler that >handles dependency resolution, job monitoring, and retries.
>Once an extract, tranform and load jobs has run, glue also uses crawlers which discover your datasets, discover your file types, extract the schema and store all this information in a centralised metadata catalogue for querying and analysis.
>You can schedule glue jobs automatically for use cases such as (but not limited to) calling APIs, automating emails and ETL.
><br>
[Source](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)

## Athena
Athena is an interactive query service that makes it easy to analyse data in Amazon S3 using standard structured query language (SQL).
Athena is useful when initially investigating a dataset or performing ad hoc data analysis.
You are charged $5 per terabyte scanned by your queries so it is best practice to limit your query to the top 20 rows of data.
`limit 20`

## Jupyter Notebook via Amazon Elastic MapReduce (EMR)
### Amazon Elastic MapReduce (EMR)
EMR allows you to spin up a cluster of computers to process big data frameworks, such as Apache Spark, on Amazon Web Services, to process and analyse vast amounts of data quickly.
When you spin up a cluster, you can choose how many master and worker nodes (computers) you would like to do the work.
Master nodes interpret code provided and divide it up between the workers. For this project you typically want 1 master and 3 workers machines when spinning up a cluster.
We use the below master and worker instance types, including pricing per DPU (data processing unit) hour:
Node type | Instance type | Instance count | Spot price (per DPU hour)
--- | --- | --- | ---
Master | m5.xlarge | 1 | $0.078
Core | m5.xlarge | 3 | $0.078

### Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

To use jupyter, you must first connect to a cluster which was created in EMR as these computers will be used to process the data.
Jupyter is best utilised for researching and developing new extract, transform and load (ETL) jobs and statistical or machine learning models. ```

# Visual Studio Code (VS Code)
Visual Studio Code is an integrated development environment (IDE).
Visual Studio Code allows us to turn our proof of concept Jupyter notebooks into productionised, tested and documented PySpark Jobs. We can then execute these jobs via Glue.
Visual Studio Code allows you to write tests for your code to ensure it is working correctly alongside having a debug mode which will let you know if there is an issue with the code itself or if a test fails.

# Git
Git is a version control system which records changes to our code; this is held in a repository.
Git makes it easy to see who has changed code and why - allowing several people to work on the same project and showing the changes each person has made.
It is easy to revert changes to code on github to a previous version.
Using git, you can download a copy of a project onto your computer, work on it locally and push the changes up to the main branch.

# Circleci
Circle CI is the continuous integration / continuous deployment (CI/CD) tool we utilise for our automated testing and deployment pipeline. It is responsible for ensuring our tests are passing before merging git branches and deploying our scripts to Amazon Web Services S3.
