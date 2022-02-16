# Use Cases
Tool | Use Case
--- | --- 
S3 | Storing Data
Glue | Job automation, ETL, Scheduling, Metadata
Athena | Adhoc querying and investigation using SQL
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
>AWS Glue is a fully managed ETL (extract, transform, and load) service.
>Using Glue, you can categorize your data, clean it, enrich it, and move it between various data stores and data streams. 
>AWS Glue consists of a central metadata repository known as the AWS Glue Data Catalog, an ETL engine that automatically generates Python or Scala code, and a scheduler that >handles dependency resolution, job monitoring, and retries.
>Once an ETL jobs has run, glue also uses crawlers which discover your datasets, discover your file types, extract the schema and store all this information in a centralised >metadata catalogue for querying and analysis.
>You can schedule glue jobs automatically for use cases such as (but not limited to) calling APIs, automating emails and ETL).
><br>
[Source](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)

## Athena
Athena is an interactive query service that makes it easy to analyse data in Amazon S3 using standard SQL. 
Athena is useful when initially investigating a dataset or performing ad hoc data analysis.
You are charged $5 per terabyte scanned by your queries so it is best practice to limit your query to the top 20 rows of data.
`limit 20`

## Jupyter Notebook via Amazon EMR
### Amazon EMR
Amazon EMR (elastic map reduce) allows you to spin up a cluster of computers to process big data frameworks, such as Apache Spark, on AWS, to process and analyse vast amounts of data quickly.
When you spin up a cluster, you can choose how many master and worker nodes (computers) you would like to do the work.
Master nodes interpret code provided and divide it up between the workers. For this project you typically want 1 master and 3 workers machines when spinning up a cluster.
<br>
### Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

To use jupyter, you must first connect to a cluster which was created in EMR as these computers will be used to process the data.
Jupyter is best utilised for researching and developing new ETL jobs and statistical or machine learning models. ```

# VS Code
VSCode is an IDE (integrated development environment).
VSCode allows us to turn our proof of concept Jupyter notebooks into productionised, tested and documented PySpark Jobs. We can then execute these jobs via Glue.
VSCode allows you to write tests for your code to ensure it is working correctly alongside having a debug mode which will let you know if there is an issue with the code itself or if a test fails.

# Circleci
Circle CI is the CI/CD (continuous integration / continuous deployment) tool we utilise for our automated testing and deployment pipeline. It is responsible for ensuring our tests are passing before merging git branches and deploying our scripts to AWS S3.

# Use Cases
Tool | Use Case
--- | --- 
S3 | Storing Data
Glue | Job automation, ETL, API connection
Athena | Initial dataset investigation using SQL
Jupyter | Pre-production environment, useful for investigation
VS Code | Production/development environment, develop notebooks into automated, tested and documented jobs.
Circleci | Devops/infrastructure - automated testing and deployment

