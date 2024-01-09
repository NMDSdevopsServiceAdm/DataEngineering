import pandas as pd
from pathlib import Path



DIRECTORY = Path(
    "F:/ASC-WDS Copy Files/Research & Analysis Team Folders/Analysis Team/b. Data Sources/02. Data sets and databases/CQC Registered Providers lists/"
)
OLD_FILE_NAME = Path("2024/01. CQC 050124.xlsx")
NEW_FILE_NAME = Path("2024/01. CQC 050124 inc sector and service.xlsx")
SHEET_NAME = "HSCA_Active_Locations"
BLANK_ROWS = 6  # CQC data file starts on row 7
COLUMNS_TO_EXPORT = ["Location ID", "Sector", "Main Service"]
SERVICE_TYPES = [
    "Service type - Shared Lives",
    "Service type - Care home service with nursing",
    "Service type - Care home service with nursing",
    "Service type - Care home service without nursing",
    "Service type - Domiciliary care service",
    "Service type - Community health care services - Nurses Agency only",
    "Service type - Supported living service",
    "Service type - Extra Care housing services",
    "Service type - Residential substance misuse treatment and/or rehabilitation service",
    "Service type - Hospice services",
    "Service type - Acute services with overnight beds",
    "Service type - Rehabilitation services",
    "Service type - Community based services for people with a learning disability",
    "Service type - Community based services for people who misuse substances",
    "Service type - Community healthcare service",
    "Service type - Diagnostic and/or screening service",
    "Service type - Community based services for people with mental health needs",
    "Service type - Long term conditions services",
    "Service type - Hospital services for people with mental health needs, learning disabilities and problems with substance misuse",
    "Service type - Doctors consultation service",
    "Service type - Doctors treatment service",
    "Service type - Dental service",
    "Service type - Urgent care services",
    "Service type - Mobile doctors service",
    "Service type - Remote clinical advice service",
    "Service type - Acute services without overnight beds / listed acute services with or without overnight beds",
    "Service type - Ambulance service",
    "Service type - Blood and Transplant service",
    "Service type - Diagnostic and/or screening service - single handed sessional providers",
    "Service type - Hospice services at home",
    "Service type - Hyperbaric Chamber",
    "Service type - Prison Healthcare Services",
    "Service type - Specialist college service",
]
SERVICE_DICT = {
    "Service type - Shared Lives": "CQC Shared Lives",
    "Service type - Care home service with nursing": "CQC Care home with nursing",
    "Service type - Care home service without nursing": "CQC Care only home",
    "Service type - Domiciliary care service": "CQC Non residential",
    "Service type - Community health care services - Nurses Agency only": "CQC Non residential",
    "Service type - Supported living service": "CQC Non residential",
    "Service type - Extra Care housing services": "CQC Non residential",
    "Service type - Residential substance misuse treatment and/or rehabilitation service": "CQC Other Residential",
    "Service type - Hospice services": "CQC Other Residential",
    "Service type - Acute services with overnight beds": "CQC Other Residential",
    "Service type - Rehabilitation services": "CQC Other non-res",
    "Service type - Community based services for people with a learning disability": "CQC Other non-res",
    "Service type - Community based services for people who misuse substances": "CQC Other non-res",
    "Service type - Community healthcare service": "CQC Other non-res",
    "Service type - Diagnostic and/or screening service": "CQC Other non-res",
    "Service type - Community based services for people with mental health needs": "CQC Other non-res",
    "Service type - Long term conditions services": "CQC Other non-res",
    "Service type - Hospital services for people with mental health needs, learning disabilities and problems with substance misuse": "CQC Other non-res",
    "Service type - Doctors consultation service": "CQC Other non-res",
    "Service type - Doctors treatment service": "CQC Other non-res",
    "Service type - Dental service": "CQC Other non-res",
    "Service type - Urgent care services": "CQC Other non-res",
    "Service type - Mobile doctors service": "CQC Other non-res",
    "Service type - Remote clinical advice service": "CQC Other non-res",
    "Service type - Acute services without overnight beds / listed acute services with or without overnight beds": "CQC Other non-res",
    "Service type - Ambulance service": "CQC Other non-res",
    "Service type - Blood and Transplant service": "CQC Other non-res",
    "Service type - Diagnostic and/or screening service - single handed sessional providers": "CQC Other non-res",
    "Service type - Hospice services at home": "CQC Other non-res",
    "Service type - Hyperbaric Chamber": "CQC Other non-res",
    "Service type - Prison Healthcare Services": "CQC Other non-res",
    "Service type - Specialist college service": "Exclude",
}


def main():
    file = DIRECTORY / OLD_FILE_NAME
    data = save_CQC_file(
        add_service_data(add_sector_data(open_CQC_File(file, SHEET_NAME)))
    )
    print("complete")


### This function opens CQC data file as pandas frame. This function takes 2 inputs (OLD_File_name and sheet_name)and returns a dataframe with CQC data.
def open_CQC_File(File_name, sheet_name):
    print("opening file")
    # opens file, skiprows required as data starts on row 7
    CQCdata = pd.read_excel(File_name, sheet_name=sheet_name, skiprows=BLANK_ROWS)

    # only keep social care data
    CQCdata = CQCdata[CQCdata["Location Type/Sector"] == "Social Care Org"].reset_index(
        drop=True
    )

    # append combines this file with files already imported
    # CQCfiles.append(CQCdata)
    return CQCdata


### This function adds SECTOR data to the dataframe. It takes the old dataframe as an input and returns an updated dataframe.
def add_sector_data(sector_data):
    print("adding sector")
    sector_data["Sector"] = sector_data["Provider Name"].str.contains(
        "Department of Community Services|Social & Community Services|SCC Adult Social Care|Cheshire West and Chester Reablement Service|Council|Social Services|MBC|MDC|London Borough|Royal Borough|Borough of",
        case=False,
        regex=True,
    ) & ~sector_data["Provider Name"].str.contains(
        "The Council of St Monica Trust", case=False, regex=True
    )

    sector_data["Sector"] = sector_data["Sector"].map(
        {True: "Local authority", False: "Independent"}
    )
    return sector_data


### This function determines the main service for a row. It takes a row (series object) from the dataframe and returns a string with the main service.
def get_main_service(row):
    service = "_not found_"

    for service_type in SERVICE_TYPES:
        if row[service_type] == "Y":
            service = service_type
            return service
    else:
        return service


### This function adds MAIN SERVICE data to the dataframe. It take the old dataframe as an input and outputs an updated dataframe.
def add_service_data(service_data):
    print("adding main service")
    service_data.loc[:, "Main Service"] = service_data.apply(get_main_service, axis=1)

    service_data["Main Service Group"] = service_data["Main Service"].map(SERVICE_DICT)

    return service_data


### This function saves selected columns from the dataframe to a new Excel Workbook. It takes a dataframe and reurtns None.
def save_CQC_file(df):
    print("saving file")
    save_location = DIRECTORY / NEW_FILE_NAME
    df.to_excel(save_location, sheet_name="CQC sector service", index=False)


if __name__ == "__main__":
    main()
