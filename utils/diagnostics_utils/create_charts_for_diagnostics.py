import boto3
import io
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import axes
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure
from pyspark.sql import DataFrame


from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    EstimateFilledPostsSource,
)

list_of_charts: list = [
    IndCqc.estimate_value,
    IndCqc.absolute_residual,
    IndCqc.standardised_residual,
    IndCqc.percentage_residual,
]


def create_charts_for_care_home_model_diagnostics(
    df: DataFrame, destination: str
) -> None:
    care_home_df = df.where(
        (
            (df[IndCqc.primary_service_type] == PrimaryServiceType.care_home_only)
            | (
                df[IndCqc.primary_service_type]
                == PrimaryServiceType.care_home_with_nursing
            )
        )
        & (df[IndCqc.estimate_source] == EstimateFilledPostsSource.care_home_model)
    )

    with io.BytesIO() as output:
        with PdfPages(output) as pp:
            for chart in list_of_charts:
                fig = create_figure(care_home_df, chart)
                pp.savefig(fig)
        data = output.getvalue()

    print(f"charts bucket destination: {destination}")
    s3 = boto3.resource("s3")
    dir = "domain=charts/"
    file = dir + "care_home_diagnostics.pdf"
    s3.Bucket(destination).put_object(Key=file, Body=data)
    return


def create_figure(df: DataFrame, chart: str) -> Figure:
    data = np.array(df.select(chart).collect()).reshape(-1)
    fig, ax = plt.subplots()
    ax.hist(data, align="left")
    ax.set_title(
        f"Histogram of {chart} for {EstimateFilledPostsSource.care_home_model}"
    )
    ax.set_ylabel("n")
    ax.set_xlabel(f"{EstimateFilledPostsSource.care_home_model} value")
    return fig
