output "event_rule_name" {
  description = "The name of an event rule for adding a csv"
  value       = aws_cloudwatch_event_rule.csv_added.name
}