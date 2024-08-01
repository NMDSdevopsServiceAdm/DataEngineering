from pyspark.sql import DataFrame
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def create_charts_for_diagnostics(df: DataFrame, destination: str) -> None:
    data = np.array(df.select(IndCqc.estimate_value).collect()).reshape(-1)
    other_data = np.array(df.select(IndCqc.absolute_residual).collect()).reshape(-1)
    with PdfPages(destination) as pdf:
        fig, ax = plt.subplots()
        ax.hist(data)
        pdf.savefig(fig)
        other_fig, other_ax = plt.subplots()
        other_ax.hist(other_data)
        pdf.savefig(other_fig)
    return
