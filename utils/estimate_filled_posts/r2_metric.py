from pyspark.ml.evaluation import RegressionEvaluator
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def generate_r2_metric(df, prediction, label):
    model_evaluator = RegressionEvaluator(
        predictionCol=prediction, labelCol=label, metricName=IndCqc.r2
    )

    r2 = model_evaluator.evaluate(df)
    print("Calculating R Squared (R2) = %g" % r2)

    return r2
