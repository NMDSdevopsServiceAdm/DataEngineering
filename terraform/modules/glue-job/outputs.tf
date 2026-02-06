output "job_name" {
  description = "The name of a glue job"
  value       = aws_glue_job.glue_job.name
}

output "job_arn" {
  description = "The ARN of a glue job"
  value       = aws_glue_job.glue_job.arn
}
