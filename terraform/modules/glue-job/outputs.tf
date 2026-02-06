output "job_name" {
  value = aws_glue_job.glue_job.name
  description = "The name of a glue job"
}

output "job_arn" {
  value = aws_glue_job.glue_job.arn
  description = "The ARN of a glue job"
}
