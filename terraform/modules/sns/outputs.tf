output "topic_arn" {
  description = "The ARN of an SNS topic"
  value       = aws_sns_topic.sns_topic.arn
}