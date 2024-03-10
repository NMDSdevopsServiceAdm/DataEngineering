import pyspark.sql.functions as F


def update_dataframe_with_identifying_rule(input_df, rule_name, output_column_name):
    source_output_column_name = output_column_name + "_source"
    return input_df.withColumn(
        source_output_column_name,
        F.when(
            (
                F.col(output_column_name).isNotNull()
                & F.col(source_output_column_name).isNull()
            ),
            rule_name,
        ).otherwise(F.col(source_output_column_name)),
    )
