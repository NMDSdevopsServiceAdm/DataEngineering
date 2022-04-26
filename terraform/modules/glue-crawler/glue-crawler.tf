resource "aws_glue_crawler" "crawler" {
  database_name = "${terraform.workspace}-data_engineering_db"
  name          = "${terraform.workspace}-data_engineering_${var.datset_for_crawler}"
  role          = var.glue_crawler_iam_role
  schedule      = var.schedule

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  s3_target {
    path = var.data_path
  }

  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode(
    {
      "Version" : 1.0,
      "Grouping" = {
        "TableLevelConfiguration" = 3,
        "TableGroupingPolicy" : "CombineCompatibleSchemas"
      }
    }
  )
}