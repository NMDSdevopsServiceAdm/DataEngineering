import sys

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ons_pd.label_dictionaries import onspd_labels_dict

ONS_SCHEMA = {
    ONSClean.postcode: pl.String,
    ONSClean.cssr: pl.String,
    ONSClean.region: pl.String,
    ONSClean.sub_icb: pl.String,
    ONSClean.icb: pl.String,
    ONSClean.icb_region: pl.String,
    ONSClean.latitude: pl.String,
    ONSClean.longitude: pl.String,
    ONSClean.imd_score: pl.String,
    ONSClean.lower_super_output_area_2011: pl.String,
    ONSClean.middle_super_output_area_2011: pl.String,
    ONSClean.rural_urban_indicator_2011: pl.String,
    ONSClean.rural_urban_indicator_2021: pl.String,
    ONSClean.lower_super_output_area_2021: pl.String,
    ONSClean.middle_super_output_area_2021: pl.String,
    ONSClean.parliamentary_constituency: pl.String,
    Keys.import_date: pl.String,
}

# rural_urban_indicator_2021 deliberately absent - contemporary never carried it.
CONTEMPORARY_RENAME = {
    ONSClean.cssr: ONSClean.contemporary_cssr,
    ONSClean.region: ONSClean.contemporary_region,
    ONSClean.sub_icb: ONSClean.contemporary_sub_icb,
    ONSClean.icb: ONSClean.contemporary_icb,
    ONSClean.icb_region: ONSClean.contemporary_icb_region,
    ONSClean.latitude: ONSClean.contemporary_latitude,
    ONSClean.longitude: ONSClean.contemporary_longitude,
    ONSClean.imd_score: ONSClean.contemporary_imd_score,
    ONSClean.lower_super_output_area_2011: ONSClean.contemporary_lsoa11,
    ONSClean.middle_super_output_area_2011: ONSClean.contemporary_msoa11,
    ONSClean.rural_urban_indicator_2011: ONSClean.contemporary_rural_urban_ind_11,
    ONSClean.lower_super_output_area_2021: ONSClean.contemporary_lsoa21,
    ONSClean.middle_super_output_area_2021: ONSClean.contemporary_msoa21,
    ONSClean.parliamentary_constituency: ONSClean.contemporary_constituency,
}

CURRENT_RENAME = {
    ONSClean.contemporary_ons_import_date: ONSClean.current_ons_import_date,
    ONSClean.cssr: ONSClean.current_cssr,
    ONSClean.region: ONSClean.current_region,
    ONSClean.sub_icb: ONSClean.current_sub_icb,
    ONSClean.icb: ONSClean.current_icb,
    ONSClean.icb_region: ONSClean.current_icb_region,
    ONSClean.latitude: ONSClean.current_latitude,
    ONSClean.longitude: ONSClean.current_longitude,
    ONSClean.imd_score: ONSClean.current_imd_score,
    ONSClean.lower_super_output_area_2011: ONSClean.current_lsoa11,
    ONSClean.middle_super_output_area_2011: ONSClean.current_msoa11,
    ONSClean.rural_urban_indicator_2011: ONSClean.current_rural_urban_ind_11,
    ONSClean.rural_urban_indicator_2021: ONSClean.current_rural_urban_ind_21,
    ONSClean.lower_super_output_area_2021: ONSClean.current_lsoa21,
    ONSClean.middle_super_output_area_2021: ONSClean.current_msoa21,
    ONSClean.parliamentary_constituency: ONSClean.current_constituency,
}


def main(ons_source: str, cleaned_ons_destination: str) -> None:
    """Cleans the raw ONS Postcode Directory dataset.

    Converts each row's import date to a `contemporary_ons_import_date` date
    column, relabels coded geography columns to their human-readable values,
    then produces two geography shapes for every postcode: `contemporary_*`
    (the geography as it stood on that row's own import date, kept for every
    historical row) and `current_*` (the latest import date's geography,
    joined onto every row so historical rows also carry the up-to-date
    classification).

    Args:
        ons_source (str): source s3 directory for parquet ONS postcode
            directory dataset.
        cleaned_ons_destination (str): destination s3 directory for cleaned
            parquet ONS postcode directory dataset.
    """
    ons_lf = utils.scan_parquet(ons_source, schema=ONS_SCHEMA)

    ons_lf = cUtils.column_to_date(
        ons_lf, Keys.import_date, ONSClean.contemporary_ons_import_date
    )

    labels_lf = build_labels_lf(onspd_labels_dict)
    ons_lf = cUtils.apply_categorical_labels(
        ons_lf,
        labels_lf,
        list(onspd_labels_dict.keys()),
        add_as_new_column=False,
    )

    contemporary_ons_lf = prepare_contemporary_ons_data(ons_lf)
    current_ons_lf = prepare_current_ons_data(ons_lf)

    combined_ons_lf = contemporary_ons_lf.join(
        current_ons_lf, on=ONSClean.postcode, how="left"
    )

    utils.sink_to_parquet(combined_ons_lf, output_path=cleaned_ons_destination)


def prepare_contemporary_ons_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Selects the geography-as-it-stood-on-that-date columns for every row.

    Args:
        lf (pl.LazyFrame): the labelled ONS postcode directory LazyFrame.

    Returns:
        pl.LazyFrame: one row per input row, with geography columns renamed
            to their `contemporary_*` equivalents.
    """
    return lf.rename(CONTEMPORARY_RENAME).select(
        ONSClean.postcode, *contemporary_geography_columns
    )


def prepare_current_ons_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Selects the latest geography snapshot, one row per postcode.

    Args:
        lf (pl.LazyFrame): the labelled ONS postcode directory LazyFrame.

    Returns:
        pl.LazyFrame: rows filtered to the maximum
            `contemporary_ons_import_date`, with geography columns renamed to
            their `current_*` equivalents.
    """
    current_lf = utils.filter_to_maximum_value_in_column(
        lf, ONSClean.contemporary_ons_import_date
    )
    return current_lf.rename(CURRENT_RENAME).select(
        ONSClean.postcode, *current_geography_columns
    )


def build_labels_lf(labels_dict: dict[str, dict[str, str]]) -> pl.LazyFrame:
    """Flattens a column-to-code-to-label dict into a labels LazyFrame.

    `apply_categorical_labels` expects a `(column_name, code, label)`
    LazyFrame; the ONS geography labels are defined as a static Python dict
    of dicts (`onspd_labels_dict`) rather than a file, so there's nothing to
    `pl.scan_csv` directly.

    Args:
        labels_dict (dict[str, dict[str, str]]): mapping of column name to a
            `{code: label}` dict for that column.

    Returns:
        pl.LazyFrame: one row per (column_name, code, label) triple.
    """
    rows = [
        (column, code, label)
        for column, mapping in labels_dict.items()
        for code, label in mapping.items()
    ]
    return pl.LazyFrame(
        rows, schema=[DLC.column_name, DLC.code, DLC.label], orient="row"
    )


if __name__ == "__main__":
    print(f"Fargate job 'clean_ons_data' called with parameters: {sys.argv}")

    args = utils.get_args(
        (
            "--ons_source",
            "Source s3 directory for parquet ONS postcode directory dataset",
        ),
        (
            "--cleaned_ons_destination",
            "Destination s3 directory for cleaned parquet ONS postcode directory dataset",
        ),
    )

    main(args.ons_source, args.cleaned_ons_destination)
    print("Fargate job 'clean_ons_data' complete")
