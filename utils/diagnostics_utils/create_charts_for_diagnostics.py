from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    EstimateFilledPostsSource,
)

list_of_charts: list = [IndCqc.estimate_value, IndCqc.absolute_residual]


def create_charts_for_diagnostics(df: DataFrame, destination: str) -> None:
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
    with PdfPages(destination) as pdf:
        for chart in list_of_charts:
            fig = create_figure(care_home_df, chart)
            pdf.savefig(fig)
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
