from pyspark.ml.evaluation import RegressionEvaluator


def generate_r2_metric(df, prediction, label):
    model_evaluator = RegressionEvaluator(
        predictionCol=prediction, labelCol=label, metricName="r2"
    )

    r2 = model_evaluator.evaluate(df)
    print("Calculating R Squared (R2) = %g" % r2)

    return r2
