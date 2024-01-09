import pandas as pd

from cqc_metadata import (
    CqcCategories,
    CqcConfig,
    ColumnNames as Columns,
    ColumnValues as Values,
)


def main():
    file = CqcConfig.DIRECTORY / CqcConfig.OLD_FILE_NAME
    data = open_CQC_File(file, CqcConfig.SHEET_NAME)
    data = remove_non_social_care_data(data)
    data = add_sector_data(data)
    data = add_service_data(data)
    data = save_CQC_file(data)
    print("complete")


def open_CQC_File(File_name, sheet_name):
    print("opening file")
    CQCdata = pd.read_excel(
        File_name, sheet_name=sheet_name, skiprows=CqcConfig.BLANK_ROWS
    )
    return CQCdata


def remove_non_social_care_data(data):
    data = data[data[Columns.location_type] == Values.social_care_org].reset_index(
        drop=True
    )
    return data


def add_sector_data(data):
    print("adding sector")
    data[Columns.sector] = data[Columns.provider_name].str.contains(
        CqcCategories.la_keywords,
        case=False,
        regex=True,
    ) & ~data[Columns.provider_name].str.contains(
        CqcCategories.non_la_keywords, case=False, regex=True
    )

    data[Columns.sector] = data[Columns.sector].map(
        {True: Values.local_authority, False: Values.independent}
    )
    return data


def get_main_service(row):
    service = Values.service_not_found

    for service_type in CqcCategories.SERVICE_TYPES:
        if row[service_type] == Values.yes:
            service = service_type
            return service
    else:
        return service


def add_service_data(data):
    print("adding main service")
    data.loc[:, Columns.main_service] = data.apply(get_main_service, axis=1)

    data[Columns.main_service_group] = data[Columns.main_service].map(
        CqcCategories.SERVICE_DICT
    )

    return data


def save_CQC_file(df):
    print("saving file")
    save_location = CqcConfig.DIRECTORY / CqcConfig.NEW_FILE_NAME
    df.to_excel(save_location, sheet_name=CqcConfig.new_sheet_name, index=False)


if __name__ == "__main__":
    main()
