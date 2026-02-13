output "lambda_arn" {
  description = "The ARN of a lambda"
  value       = aws_lambda_function.lambda_notification.arn
}