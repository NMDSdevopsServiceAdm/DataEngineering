from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONS,
)


@dataclass
class Region:
    """The possible values of the region columns in ONS data"""

    east_midlands: str = "East Midlands"
    eastern: str = "Eastern"
    london: str = "London"
    north_east: str = "North East"
    north_west: str = "North West"
    south_east: str = "South East"
    south_west: str = "South West"
    west_midlands: str = "West Midlands"
    yorkshire_and_the_humber: str = "Yorkshire and the Humber"

    values_list = [
        east_midlands,
        eastern,
        london,
        north_east,
        north_west,
        south_east,
        south_west,
        west_midlands,
        yorkshire_and_the_humber,
    ]


@dataclass
class CurrentRegion(Region):
    """The possible values of the current region column in ONS data"""

    column_name: str = ONS.current_region


@dataclass
class ContemporaryRegion(Region):
    """The possible values of the contemporary region column in ONS data"""

    column_name: str = ONS.contemporary_region


@dataclass
class RUI:
    """The possible values of the rural urban indicator columns in ONS data"""

    rural_hamlet_sparse: str = "Rural hamlet and isolated dwellings in a sparse setting"
    rural_hamlet: str = "Rural hamlet and isolated dwellings"
    rural_village: str = "Rural village"
    rural_town_sparse: str = "Rural town and fringe in a sparse setting"
    rural_town: str = "Rural town and fringe"
    urban_city_sparse: str = "Urban city and town in a sparse setting"
    urban_city: str = "Urban city and town"
    urban_major: str = "Urban major conurbation"
    urban_minor: str = "Urban minor conurbation"
    rural_village_sparse: str = "Rural village in a sparse setting"

    values_list = [
        rural_hamlet,
        rural_hamlet_sparse,
        rural_village,
        rural_village_sparse,
        rural_town,
        rural_town_sparse,
        urban_city,
        urban_city_sparse,
        urban_major,
        urban_minor,
    ]


@dataclass
class CurrentRUI(RUI):
    """The possible values of the current rural urban indicator column in ONS data"""

    column_name: str = ONS.current_rural_urban_ind_11


@dataclass
class ContemporaryRUI(RUI):
    """The possible values of the contemporary rural urban indicator column in ONS data"""

    column_name: str = ONS.contemporary_rural_urban_ind_11


@dataclass
class CSSR:
    """The possible values of the local authority columns in ONS data"""

    barking_and_dagenham: str = "Barking & Dagenham"
    barnet: str = "Barnet"
    barnsley: str = "Barnsley"
    bath_and_north_east_somerset: str = "Bath and North East Somerset"
    bedford: str = "Bedford"
    bexley: str = "Bexley"
    birmingham: str = "Birmingham"
    blackburn_with_darwen: str = "Blackburn with Darwen"
    blackpool: str = "Blackpool"
    bolton: str = "Bolton"
    bournemouth: str = "Bournemouth"
    bournemouth_christchurch_and_poole: str = "Bournemouth Christchurch and Poole"
    bracknell_forest: str = "Bracknell Forest"
    bradford: str = "Bradford"
    brent: str = "Brent"
    brighton_and_hove: str = "Brighton & Hove"
    bristol: str = "Bristol"
    bromley: str = "Bromley"
    buckinghamshire: str = "Buckinghamshire"
    bury: str = "Bury"
    calderdale: str = "Calderdale"
    cambridgeshire: str = "Cambridgeshire"
    camden: str = "Camden"
    central_bedfordshire: str = "Central Bedfordshire"
    cheshire_east: str = "Cheshire East"
    cheshire_west_and_chester: str = "Cheshire West & Chester"
    city_of_london: str = "City of London"
    cornwall: str = "Cornwall"
    coventry: str = "Coventry"
    croydon: str = "Croydon"
    cumberland: str = "Cumberland"
    cumbria: str = "Cumbria"
    darlington: str = "Darlington"
    derby: str = "Derby"
    derbyshire: str = "Derbyshire"
    devon: str = "Devon"
    doncaster: str = "Doncaster"
    dorset: str = "Dorset"
    dudley: str = "Dudley"
    durham: str = "Durham"
    ealing: str = "Ealing"
    east_riding_of_yorkshire: str = "East Riding of Yorkshire"
    east_sussex: str = "East Sussex"
    enfield: str = "Enfield"
    essex: str = "Essex"
    gateshead: str = "Gateshead"
    gloucestershire: str = "Gloucestershire"
    greenwich: str = "Greenwich"
    hackney: str = "Hackney"
    halton: str = "Halton"
    hammersmith_and_fulham: str = "Hammersmith & Fulham"
    hampshire: str = "Hampshire"
    haringey: str = "Haringey"
    harrow: str = "Harrow"
    hartlepool: str = "Hartlepool"
    havering: str = "Havering"
    herefordshire: str = "Herefordshire"
    hertfordshire: str = "Hertfordshire"
    hillingdon: str = "Hillingdon"
    hounslow: str = "Hounslow"
    isle_of_wight: str = "Isle of Wight"
    isles_of_scilly: str = "Isles of Scilly"
    islington: str = "Islington"
    kensington_and_chelsea: str = "Kensington & Chelsea"
    kent: str = "Kent"
    kingston_upon_hull: str = "Kingston upon Hull"
    kingston_upon_thames: str = "Kingston upon Thames"
    kirklees: str = "Kirklees"
    knowsley: str = "Knowsley"
    lambeth: str = "Lambeth"
    lancashire: str = "Lancashire"
    leeds: str = "Leeds"
    leicester: str = "Leicester"
    leicestershire: str = "Leicestershire"
    lewisham: str = "Lewisham"
    lincolnshire: str = "Lincolnshire"
    liverpool: str = "Liverpool"
    luton: str = "Luton"
    manchester: str = "Manchester"
    medway: str = "Medway"
    merton: str = "Merton"
    middlesbrough: str = "Middlesbrough"
    milton_keynes: str = "Milton Keynes"
    newcastle_upon_tyne: str = "Newcastle upon Tyne"
    newham: str = "Newham"
    norfolk: str = "Norfolk"
    north_east_lincolnshire: str = "North East Lincolnshire"
    north_lincolnshire: str = "North Lincolnshire"
    north_northamptonshire: str = "North Northamptonshire"
    north_somerset: str = "North Somerset"
    north_tyneside: str = "North Tyneside"
    north_yorkshire: str = "North Yorkshire"
    northamptonshire: str = "Northamptonshire"
    northumberland: str = "Northumberland"
    nottingham: str = "Nottingham"
    nottinghamshire: str = "Nottinghamshire"
    oldham: str = "Oldham"
    oxfordshire: str = "Oxfordshire"
    peterborough: str = "Peterborough"
    plymouth: str = "Plymouth"
    poole: str = "Poole"
    portsmouth: str = "Portsmouth"
    reading: str = "Reading"
    redbridge: str = "Redbridge"
    redcar_and_cleveland: str = "Redcar & Cleveland"
    richmond_upon_thames: str = "Richmond upon Thames"
    rochdale: str = "Rochdale"
    rotherham: str = "Rotherham"
    rutland: str = "Rutland"
    salford: str = "Salford"
    sandwell: str = "Sandwell"
    sefton: str = "Sefton"
    sheffield: str = "Sheffield"
    shropshire: str = "Shropshire"
    slough: str = "Slough"
    solihull: str = "Solihull"
    somerset: str = "Somerset"
    south_gloucestershire: str = "South Gloucestershire"
    south_tyneside: str = "South Tyneside"
    southampton: str = "Southampton"
    southend_on_sea: str = "Southend on Sea"
    southwark: str = "Southwark"
    st_helens: str = "St Helens"
    staffordshire: str = "Staffordshire"
    stockport: str = "Stockport"
    stockton_on_tees: str = "Stockton on Tees"
    stoke_on_trent: str = "Stoke on Trent"
    suffolk: str = "Suffolk"
    sunderland: str = "Sunderland"
    surrey: str = "Surrey"
    sutton: str = "Sutton"
    swindon: str = "Swindon"
    tameside: str = "Tameside"
    telford_and_wrekin: str = "Telford & Wrekin"
    thurrock: str = "Thurrock"
    torbay: str = "Torbay"
    tower_hamlets: str = "Tower Hamlets"
    trafford: str = "Trafford"
    wakefield: str = "Wakefield"
    walsall: str = "Walsall"
    waltham_forest: str = "Waltham Forest"
    wandsworth: str = "Wandsworth"
    warrington: str = "Warrington"
    warwickshire: str = "Warwickshire"
    west_berkshire: str = "West Berkshire"
    west_northamptonshire: str = "West Northamptonshire"
    west_sussex: str = "West Sussex"
    westminster: str = "Westminster"
    westmorland_and_furness: str = "Westmorland and Furness"
    wigan: str = "Wigan"
    wiltshire: str = "Wiltshire"
    windsor_and_maidenhead: str = "Windsor & Maidenhead"
    wirral: str = "Wirral"
    wokingham: str = "Wokingham"
    wolverhampton: str = "Wolverhampton"
    worcestershire: str = "Worcestershire"
    york: str = "York"

    values_list = [
        barking_and_dagenham,
        barnet,
        barnsley,
        bath_and_north_east_somerset,
        bedford,
        bexley,
        birmingham,
        blackburn_with_darwen,
        blackpool,
        bolton,
        bournemouth,
        bournemouth_christchurch_and_poole,
        bracknell_forest,
        bradford,
        brent,
        brighton_and_hove,
        bristol,
        bromley,
        buckinghamshire,
        bury,
        calderdale,
        cambridgeshire,
        camden,
        central_bedfordshire,
        cheshire_east,
        cheshire_west_and_chester,
        city_of_london,
        cornwall,
        coventry,
        croydon,
        cumberland,
        cumbria,
        darlington,
        derby,
        derbyshire,
        devon,
        doncaster,
        dorset,
        dudley,
        durham,
        ealing,
        east_riding_of_yorkshire,
        east_sussex,
        enfield,
        essex,
        gateshead,
        gloucestershire,
        greenwich,
        hackney,
        halton,
        hammersmith_and_fulham,
        hampshire,
        haringey,
        harrow,
        hartlepool,
        havering,
        herefordshire,
        hertfordshire,
        hillingdon,
        hounslow,
        isle_of_wight,
        isles_of_scilly,
        islington,
        kensington_and_chelsea,
        kent,
        kingston_upon_hull,
        kingston_upon_thames,
        kirklees,
        knowsley,
        lambeth,
        lancashire,
        leeds,
        leicester,
        leicestershire,
        lewisham,
        lincolnshire,
        liverpool,
        luton,
        manchester,
        medway,
        merton,
        middlesbrough,
        milton_keynes,
        newcastle_upon_tyne,
        newham,
        norfolk,
        north_east_lincolnshire,
        north_lincolnshire,
        north_northamptonshire,
        north_somerset,
        north_tyneside,
        north_yorkshire,
        northamptonshire,
        northumberland,
        nottingham,
        nottinghamshire,
        oldham,
        oxfordshire,
        peterborough,
        plymouth,
        poole,
        portsmouth,
        reading,
        redbridge,
        redcar_and_cleveland,
        richmond_upon_thames,
        rochdale,
        rotherham,
        rutland,
        salford,
        sandwell,
        sefton,
        sheffield,
        shropshire,
        slough,
        solihull,
        somerset,
        south_gloucestershire,
        south_tyneside,
        southampton,
        southend_on_sea,
        southwark,
        st_helens,
        staffordshire,
        stockport,
        stockton_on_tees,
        stoke_on_trent,
        suffolk,
        sunderland,
        surrey,
        sutton,
        swindon,
        tameside,
        telford_and_wrekin,
        thurrock,
        torbay,
        tower_hamlets,
        trafford,
        wakefield,
        walsall,
        waltham_forest,
        wandsworth,
        warrington,
        warwickshire,
        west_berkshire,
        west_northamptonshire,
        west_sussex,
        westminster,
        westmorland_and_furness,
        wigan,
        wiltshire,
        windsor_and_maidenhead,
        wirral,
        wokingham,
        wolverhampton,
        worcestershire,
        york,
    ]


@dataclass
class CurrentCSSR(CSSR):
    """The possible values of the current local authority column in ONS data"""

    column_name: str = ONS.current_cssr


@dataclass
class ContemporaryCSSR(CSSR):
    """The possible values of the contemporary local authority column in ONS data"""

    column_name: str = ONS.contemporary_cssr
