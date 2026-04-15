from dataclasses import dataclass
from pathlib import Path


@dataclass
class CqcCategories:
    services_dict = {
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


@dataclass
class CqcConfig:
    directory = Path(
        "F:/03. Data sources/07. External data sets/CQC Registered Providers lists/"
    )
    source_suffix = ".xlsx"
    dest_suffix = " inc sector and service.xlsx"
    sheet_name = "HSCA_Active_Locations"
    columns_to_export = ["Location ID", "Sector", "Main Service"]
    new_sheet_name = "CQC sector service"


@dataclass
class ColumnNames:
    sector = "sector"
    provider_name = "Provider Name"
    provider_id = "Provider ID"
    location_type = "Location Type/Sector"
    main_service = "Main Service"
    main_service_group = "Main Service Group"


@dataclass
class ColumnValues:
    local_authority = "Local authority"
    independent = "Independent"
    yes = "Y"
    service_not_found = "_not found_"
    social_care_org = "Social Care Org"


# Note - also saved in `utils\cqc_local_authority_provider_ids.py`
@dataclass
class LocalAuthorityProviderIds:
    known_ids = [
        "1-101668892",    # Barking & Dagenham
        "1-101669146",    # Barnet
        "1-8921644268",   # Barnet
        "1-101669254",    # Barnsley
        "1-101646129",    # Bath & North East Somerset
        "1-9238510047",   # Bath & North East Somerset
        "1-101657852",    # Bedford
        "1-101680319",    # Bexley
        "1-101676649",    # Birmingham
        "1-101668399",    # Blackburn with Darwen
        "1-101679227",    # Blackpool
        "1-101656323",    # Bolton
        "1-101667931",    # Bournemouth
        "1-6487034000",   # Bournemouth, Christchurch & Poole
        "1-101615825",    # Bracknell Forest
        "1-101640436",    # Bradford
        "1-101668059",    # Brent
        "1-119231704",    # Brent
        "1-101611279",    # Brighton & Hove
        "1-101642256",    # Bristol
        "1-101683067",    # Bromley
        "1-101679111",    # Buckinghamshire
        "1-3527893177",   # Buckinghamshire
        "1-101668963",    # Bury
        "1-101615434",    # Calderdale
        "1-101676004",    # Cambridge
        "1-101681261",    # Cambridgeshire
        "1-101615530",    # Camden
        "1-101694068",    # Central Bedfordshire
        "1-101662593",    # Cheshire East
        "1-119231615",    # Cheshire West & Chester
        "1-101670161",    # City of London
        "1-101668779",    # Cornwall
        "1-101670082",    # Coventry
        "1-101646191",    # Croydon
        "1-15197643879",  # Cumberland
        "1-101675941",    # Cumbria
        "1-2138115141",   # Cumbria
        "1-101659850",    # Darlington
        "1-101675129",    # Derby
        "1-101668364",    # Derbyshire
        "1-101640000",    # Devon
        "1-101663854",    # Doncaster
        "1-101667966",    # Dorset
        "1-5910778375",   # Dorset
        "1-101671779",    # Dudley
        "1-101667827",    # Durham
        "1-101608688",    # Ealing
        "1-101668295",    # East Riding of Yorkshire
        "1-101642587",    # East Sussex
        "1-101615442",    # Enfield
        "1-9298511641",   # Enfield
        "1-101648089",    # Essex
        "1-101668936",    # Gateshead
        "1-101608481",    # Gloucestershire
        "1-101642042",    # Greenwich
        "1-101615460",    # Hackney
        "1-101669006",    # Halton
        "1-101668269",    # Hammersmith & Fulham
        "1-101663766",    # Hampshire
        "1-101677280",    # Hampshire
        "1-101669861",    # Haringey
        "1-119233042",    # Harrow
        "1-101667879",    # Hartlepool
        "1-101667316",    # Havering
        "1-101646603",    # Herefordshire
        "1-1753211412",   # Herefordshire
        "1-101649033",    # Hertfordshire
        "1-101671278",    # Hillingdon
        "1-101667605",    # Hounslow
        "1-101646085",    # Isle of Wight
        "1-101615495",    # Isles of Scilly
        "1-101644992",    # Islington
        "1-101640928",    # Kensington & Chelsea
        "1-101677610",    # Kent
        "1-101668172",    # Kingston upon Hull
        "1-101668084",    # Kingston upon Thames
        "1-101615309",    # Kirklees
        "1-101648114",    # Knowsley
        "1-101642168",    # Lancashire
        "1-101668024",    # Leeds
        "1-101668479",    # Leicester
        "1-101615399",    # Leicestershire
        "1-101680840",    # Lewisham
        "1-101668990",    # Lincolnshire
        "1-101657209",    # Liverpool
        "1-101671172",    # Luton
        "1-101676792",    # Manchester
        "1-101678717",    # Manchester
        "1-101668621",    # Medway
        "1-101668076",    # Merton
        "1-101694179",    # Middlesbrough
        "1-101646495",    # Milton Keynes
        "1-101669816",    # Newcastle upon Tyne
        "1-101667948",    # Newham
        "1-101668119",    # Norfolk
        "1-101646567",    # North East Lincolnshire
        "1-101668016",    # North Lincolnshire
        "1-10200144758",  # North Northamptonshire
        "1-101677388",    # North Somerset
        "1-101674314",    # North Tyneside
        "1-101668998",    # North Yorkshire
        "1-101649972",    # Northamptonshire
        "1-4812882922",   # Northamptonshire
        "1-101675085",    # Northumberland
        "1-101660217",    # Nottingham
        "1-101668287",    # Nottinghamshire
        "1-101668216",    # Oldham
        "1-101642221",    # Oxfordshire
        "1-101667809",    # Oxfordshire
        "1-321406924",    # Peterborough
        "1-101686014",    # Plymouth
        "1-101654764",    # Poole
        "1-101669658",    # Portsmouth
        "1-101615629",    # Reading
        "1-101644365",    # Redbridge
        "1-101667844",    # Redcar & Cleveland
        "1-101678236",    # Richmond upon Thames
        "1-101615477",    # Rochdale
        "1-101615664",    # Rotherham
        "1-101662540",    # Rutland
        "1-101615789",    # Salford
        "1-13227250190",  # Salford
        "1-101668102",    # Sandwell
        "1-101668007",    # Sheffield
        "1-101665586",    # Shropshire
        "1-101648166",    # Slough
        "1-190798065",    # Solihull
        "1-101663801",    # Somerset
        "1-101620560",    # South Gloucestershire
        "1-101672301",    # South Tyneside
        "1-101694188",    # South Tyneside
        "1-101675120",    # Southampton
        "1-101672212",    # Southend
        "1-101676990",    # Southwark
        "1-101692248",    # St Helens
        "1-101651669",    # Staffordshire
        "1-101681841",    # Stockport
        "1-101609822",    # Stockton on Tees
        "1-101643435",    # Stoke on Trent
        "1-101616919",    # Suffolk
        "1-101672310",    # Sunderland
        "1-101653026",    # Surrey
        "1-101643031",    # Sutton
        "1-101661627",    # Swindon
        "1-101615417",    # Tameside
        "1-101668041",    # Telford & Wrekin
        "1-101668373",    # Thurrock
        "1-112806363",    # Tower Hamlets
        "1-101640373",    # Trafford
        "1-101670456",    # Wakefield
        "1-101668390",    # Walsall
        "1-101668189",    # Waltham Forest
        "1-101670188",    # Wandsworth
        "1-101668813",    # Warrington
        "1-101675433",    # Warwickshire
        "1-101615976",    # West Berkshire
        "1-10200144798",  # West Northamptonshire
        "1-101636869",    # West Sussex
        "1-101675228",    # Westminster
        "1-15175437162",  # Westmorland and Furness
        "1-101641764",    # Wigan
        "1-101668709",    # Wiltshire
        "1-101669693",    # Windsor and Maidenhead
        "1-101668163",    # Wirral
        "1-14913549817",  # Wirral
        "1-101646986",    # Wokingham
        "1-101668355",    # Wolverhampton
        "1-101668461",    # Worcestershire
        "1-101672764",    # Worcestershire
        "1-101615486",    # York
    ] # fmt: skip
