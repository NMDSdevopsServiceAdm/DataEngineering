resource "aws_glue_crawler" "crawler" {
  database_name = var.workspace_glue_database_name
  name          = "${local.workspace_prefix}-data_engineering_${var.dataset_for_crawler}${var.name_postfix}"
  role          = var.glue_role.arn
  schedule      = var.schedule

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  s3_target {
    path       = "s3://sfc-${local.workspace_prefix}-datasets/domain=${var.dataset_for_crawler}/"
    exclusions = var.exclusions
  }

  schema_change_policy {
    delete_behavior = "DEPRECATE_IN_DATABASE"
    update_behavior = "UPDATE_IN_DATABASE"
  }


  configuration = jsonencode(
    {
      "Version" = 1.0,
      "Grouping" = {
        "TableLevelConfiguration" = var.table_level,
        "TableGroupingPolicy"     = "CombineCompatibleSchemas"
      }
    }
  )
}
