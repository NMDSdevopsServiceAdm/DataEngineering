output "crawler_name" {
  description = "The name of the glue crawler"
  value       = aws_glue_crawler.crawler.name
}
