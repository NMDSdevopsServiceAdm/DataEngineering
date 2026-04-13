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
        "1-119231704",    # Barking & Dagenham
        "1-101675941",    # Barnet
        "1-101668998",    # Barnet
        "1-101654764",    # Barnsley
        "1-101620560",    # Bath & North East Somerset
        "1-101642256",    # Bath & North East Somerset
        "1-2138115141",   # Bedford
        "1-119231615",    # Bexley
        "1-101680319",    # Birmingham
        "1-101640000",    # Blackburn with Darwen
        "1-101663766",    # Blackpool
        "1-1753211412",   # Bolton
        "1-101694068",    # Borough of Poole
        "1-101677610",    # Bournemouth
        "1-101668621",    # Bournemouth, Christchurch & Poole (BCP)
        "1-101668936",    # Bracknell Forest
        "1-101615477",    # Bradford
        "1-101615825",    # Brent
        "1-101615629",    # Brent
        "1-101668364",    # Brighton and Hove
        "1-101643435",    # Bristol
        "1-101667879",    # Bromley
        "1-101692248",    # Buckinghamshire
        "1-101668461",    # Buckinghamshire
        "1-101649033",    # Bury
        "1-101616919",    # Calderdale
        "1-101615399",    # Cambridge
        "1-9298511641",   # Cambridgeshire
        "1-101675085",    # Camden
        "1-10200144758",  # Central Bedfordshire
        "1-101668119",    # Cheshire East
        "1-101615434",    # Cheshire West and Chester
        "1-101642168",    # City of London
        "1-101675120",    # Cornwall
        "1-101668709",    # Coventry
        "1-101672310",    # Croydon
        "1-101671278",    # Cumberland
        "1-101646191",    # Cumbria
        "1-101662593",    # Cumbria
        "1-101651669",    # Darlington
        "1-101667827",    # Derby
        "1-101668479",    # Derbyshire
        "1-101676649",    # Devon
        "1-190798065",    # Doncaster
        "1-101663801",    # Dorset
        "1-6487034000",   # Dorset
        "1-101670082",    # Dudley
        "1-101648089",    # Durham
        "1-101668024",    # Ealing
        "1-101646495",    # East Riding of Yorkshire
        "1-101668287",    # East Sussex
        "1-101660217",    # Enfield
        "1-101649972",    # Enfield
        "1-101668990",    # Essex
        "1-101646085",    # Gateshead
        "1-101615976",    # Gloucestershire
        "1-101642042",    # Greenwich
        "1-101615417",    # Hackney
        "1-101668355",    # Halton
        "1-101668102",    # Hammersmith & Fulham
        "1-101668963",    # Hampshire
        "1-101642587",    # Haringey
        "1-101677388",    # Harrow
        "1-101675129",    # Hartlepool
        "1-101663854",    # Havering
        "1-101615460",    # Herefordshire
        "1-101608481",    # Herefordshire
        "1-101679111",    # Hertfordshire
        "1-101668295",    # Hillingdon
        "1-101668892",    # Hounslow
        "1-101648114",    # Isle of Wight
        "1-101671172",    # Isles of Scilly
        "1-101644992",    # Islington
        "1-101694188",    # Kensington & Chelsea
        "1-101615530",    # Kent
        "1-101668216",    # Kingston upon Hull
        "1-101661627",    # Kingston upon Thames
        "1-101672212",    # Kirklees
        "1-101668779",    # Knowsley
        "1-101670456",    # Lancashire
        "1-101668390",    # Leeds
        "1-101671779",    # Leicester
        "1-112806363",    # Leicestershire
        "1-101653026",    # Lewisham
        "1-101676792",    # Lincolnshire
        "1-101669816",    # Liverpool
        "1-101611279",    # Luton
        "1-101668016",    # Manchester
        "1-101641764",    # Manchester
        "1-101668399",    # Medway
        "1-101667966",    # Merton
        "1-101667948",    # Middlesbrough
        "1-101679227",    # Milton Keynes
        "1-101662540",    # Newcastle-upon-Tyne
        "1-101669861",    # Newham
        "1-101674314",    # Norfolk
        "1-101642221",    # North East Lincolnshire
        "1-101686014",    # North Lincolnshire
        "1-101648166",    # North Northamptonshire
        "1-101668076",    # North Somerset
        "1-101667844",    # North Tyneside
        "1-9238510047",   # North Yorkshire
        "1-101668189",    # Northamptonshire
        "1-101667605",    # Northamptonshire
        "1-101615309",    # Northumberland
        "1-101681261",    # Nottingham
        "1-101675433",    # Nottinghamshire
        "1-101659850",    # Oldham
        "1-119233042",    # Oxfordshire
        "1-101657209",    # Oxfordshire
        "1-101636869",    # Peterborough
        "1-101680840",    # Plymouth
        "1-101681841",    # Portsmouth
        "1-101609822",    # Reading
        "1-101668813",    # Redbridge
        "1-5910778375",   # Redcar & Cleveland
        "1-101668084",    # Redditch
        "1-101669658",    # Richmond upon Thames
        "1-101668007",    # Rochdale
        "1-101694179",    # Rotherham
        "1-101656323",    # Rutland
        "1-101683067",    # Salford
        "1-101615664",    # Salford
        "1-101669693",    # Sandwell
        "1-101665586",    # Sheffield
        "1-101640436",    # Shropshire
        "1-4812882922",   # Slough
        "1-101676990",    # Solihull
        "1-101669006",    # Somerset
        "1-101668172",    # South Gloucestershire
        "1-101640373",    # South Tyneside
        "1-101657852",    # South Tyneside
        "1-101668163",    # Southampton
        "1-101615442",    # Southend
        "1-101669254",    # Southwark
        "1-101646567",    # St Helens
        "1-101668269",    # Staffordshire
        "1-101678717",    # Stockport
        "1-10200144798",  # Stockton-on-Tees
        "1-8921644268",   # Stoke-on-Trent
        "1-101615486",    # Suffolk
        "1-101667931",    # Sunderland
        "1-101678236",    # Surrey
        "1-101668373",    # Sutton
        "1-101608688",    # Swindon
        "1-321406924",    # Tameside
        "1-101670161",    # Telford & Wrekin
        "1-101644365",    # Thurrock
        "1-101667809",    # Tower Hamlets
        "1-101668041",    # Trafford
        "1-101640928",    # Wakefield
        "1-101646986",    # Walsall
        "1-101675228",    # Waltham Forest
        "1-101615495",    # Wandsworth
        "1-101643031",    # Warrington
        "1-101669146",    # Warwickshire
        "1-101670188",    # West Berkshire
        "1-101677280",    # West Northamptonshire
        "1-101646129",    # West Sussex
        "1-101615789",    # Westminster
        "1-101672764",    # Westmorland and Furness
        "1-3527893177",   # Wigan
        "1-101676004",    # Wiltshire
        "1-101646603",    # Winchester
        "1-101668059",    # Windsor and Maidenhead
        "1-101667316",    # Wirral
        "1-101672301",    # Wirral
        "1-13227250190",  # Wokingham
        "1-15175437162",  # Wolverhampton
        "1-15197643879",  # Worcestershire
        "1-14913549817",  # York
    ] # fmt: skip
