# Estimate Ind CQC Filled Posts by Job Role

## Summary
This job breaks down the overall filled posts estimates for each location, giving the number of filled posts for each job role.

## Inputs
| Variable name | s3 location | Athena name | Description |
|:--------------|:------------|:------------|:------------|
|estimated_ind_cqc_filled_posts_df|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts/|dataset_ind_cqc_estimated_filled_posts|The overall filled posts estimates for each location|
|cleaned_ascwds_worker_df|s3://sfc-[branch]-datasets/domain=ASCWDS/dataset=worker_cleaned/|dataset_worker_cleaned|The cleaned worker data which contains the job role for each worker|

## Summary of steps

- Read in filled posts estimates parquet
- Read in cleaned worker data parquet
- Count registered managers
- Count job role per establishment
- Write job role estimates to parquet

## Functions

### Functions in job
```{eval-rst}
.. automodule:: jobs.estimate_ind_cqc_filled_posts_by_job_role
   :members:
   :undoc-members:
   :exclude-members: DataFrame, Window
   :show-inheritance:
   :member-order: bysource
```
### Functions from other scripts

```{eval-rst}
.. automodule:: utils.estimate_filled_posts_by_job_role_utils.utils
   :members:
   :undoc-members:
   :exclude-members: DataFrame, Window
   :show-inheritance:
   :member-order: bysource
```


```{eval-rst}
.. autofunction:: utils.utils.read_from_parquet

```
```{eval-rst}
.. autofunction:: utils.utils.write_to_parquet

```
```{eval-rst}
.. autofunction:: utils.utils.collect_arguments

```

## Ouputs
| Variable name | s3 location | Athena name | Description |
|:--------------|:------------|:------------|:------------|
|estimated_ind_cqc_filled_posts_by_job_role|s3://sfc-[branch]-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_filled_posts_by_job_role/|dataset_ind_cqc_estimated_filled_posts_by_job_role|The number of filled posts split by job roles for each location|