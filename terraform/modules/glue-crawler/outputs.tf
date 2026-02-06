output "crawler_name" {
  value = aws_glue_crawler.crawler.name
  description = "The name of the glue crawler"
}
