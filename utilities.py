import json
import pandas as pd

################################################################################
def json_to_df(path = None, data = None):
    """
    Function: json_to_df

        Read data from a json file and format it as a pandas DataFrame

    Arguments:
       data a dictionary with nested levels
       path file path to a .json or .json.gz file

    Return:
        A pandas DataFrame
    """

    assert bool(data is not None) ^ bool(data is not None)

    if path is not None:
        if path.endswith(".gz"):
            with gzip.open(path, 'r') as f:
                file_content = f.read()
            json_str = file_content.decode("utf-8")
            data = json.loads(json_str)
        else:
            f = open(path, "r")
            data = json.load(f)
            f.close()

    x = flatten_dict(data)
    x = [(*a, str(b)) for a,b in x.items()]
    x = pd.DataFrame(x)
    x.columns = ["lvl" + str(i) for i in range(len(x.columns))]
    return x

################################################################################
def flatten_dict(nested_dict):
    """
    Function: flatten_dict
        recursivly parse a nested dictionary and generate a generic pandas
        DataFrame

    Arguments:
        nested_dict a dictionary

    Return:
        A pandas DataFrame
    """

    res = {}
    if isinstance(nested_dict, dict):
        for k in nested_dict:
            flattened_dict = flatten_dict(nested_dict[k])
            for key, val in flattened_dict.items():
                key = list(key)
                key.insert(0, k)
                res[tuple(key)] = val
    else:
        res[()] = nested_dict
    return res

################################################################################
# Define Needed Constructs

REGIONS = [
        'AIA_CZ1', 'AIA_CZ2', 'AIA_CZ3', 'AIA_CZ4', 'AIA_CZ5',
        'NYUP', 'CASO', 'PJMC', 'CANO', 'PJMD', 'RMRG', 'NYCW', 'MISE', 'MISC',
        'PJMW', 'PJME', 'SRSG', 'NWPP', 'ISNE', 'MISW', 'TRE', 'SPPN', 'SPPC',
        'SPPS', 'MISS', 'SRCE', 'FRCC', 'SRSE', 'SRCA', 'BASN',
        'WA', 'AK', 'RI', 'OK', 'AR', 'NH', 'WI', 'WY', 'IA', 'MS', 'MI', 'CA',
        'MN', 'NE', 'OH', 'NV', 'NY', 'FL', 'NM', 'CO', 'SD', 'MD', 'VA', 'SC',
        'NJ', 'MT', 'ME', 'MA', 'NC', 'OR', 'UT', 'HI', 'KS', 'CT', 'TN', 'ID',
        'ND', 'VT', 'WV', 'GA', 'LA', 'PA', 'DE', 'TX', 'AZ', 'MO', 'IL', 'DC',
        'AL', 'KY', 'IN'
        ]

BUILDING_TYPES = [
        'single family home',
        'multi family home',
        'mobile home',
        'assembly',
        'education',
        'food sales',
        'food service',
        'health care',
        'lodging',
        'large office',
        'small office',
        'mercantile/service',
        'warehouse',
        'other',
        'unspecified'
        ]

FUEL_TYPES = [
        'electricity',
        'natural gas',
        'distillate',
        'other fuel'
        ]

END_USES = [
        'heating',
        'cooling',
        'lighting',
        'MELs',
        'refrigeration',
        'other',
        'secondary heating',
        'water heating',
        'cooking',
        'ventilation',
        'TVs',
        'computers',
        'onsite generation',
        'drying',
        'PCs',
        'non-PC office equipment',
        'ceiling fan',
        'fans and pumps',
        'unspecified'
        ]

SUPPLY_DEMAND = ['supply', 'demand']
ENERGY_STOCK = ['energy', 'stock']

TECHNOLOGIES = [
        '100W Equivalent A19 Halogen',
        '100W Equivalent CFL Bare Spiral',
        '100W A19 Incandescent',
        'ASHP',
        'CAV_Vent',
        'Commercial Beverage Merchandisers',
        'Commercial Ice Machines',
        'Commercial Reach-In Freezers',
        'Commercial Reach-In Refrigerators',
        'Commercial Refrigerated Vending Machines',
        'Commercial Supermarket Display Cases',
        'Commercial Walk-In Freezers',
        'Commercial Walk-In Refrigerators',
        'GSHP',
        'Halogen Infrared Reflector (HIR) PAR38',
        'Halogen PAR38',
        'HP water heater',
        'IT equipment',
        'LED Integrated Luminaire',
        'LED PAR38',
        'Metal Halide',
        'Mercury Vapor',
        'NGHP',
        'OTT streaming devices',
        'Sodium Vapor',
        'Solar water heater',
        'T5 4xF54 HO High Bay',
        'T5 F28',
        'T8 F28',
        'T8 F32',
        'T8 F59',
        'T8 F96',
        'TV',
        'VAV_Vent',
        'boiler (NG)',
        'boiler (distillate)',
        'central AC',
        'centrifugal_chiller',
        'clothes washing',
        'coffee brewers',
        'coffee maker',
        'comm_GSHP-cool',
        'comm_GSHP-heat',
        'data center UPS',
        'dehumidifier',
        'desktop PC',
        'dishwasher',
        'distribution transformers',
        'elec_boiler',
        'elec_water_heater',
        'electric WH',
        'electric other',
        'electric_range_oven_24x24_griddle',
        'electric_res-heat',
        'elevators',
        'equipment gain',
        'escalators',
        'external (CFL)',
        'external (LED)',
        'external (high pressure sodium)',
        'external (incandescent)',
        'floor',
        'freezers',
        'fume hoods',
        'furnace (LPG)',
        'furnace (NG)',
        'furnace (distillate)',
        'gas_boiler',
        'gas_chiller',
        'gas_eng-driven_RTAC',
        'gas_eng-driven_RTHP-cool',
        'gas_eng-driven_RTHP-heat',
        'gas_furnace',
        'gas_range_oven_24x24_griddle',
        'gas_water_heater',
        'general service (CFL)',
        'general service (LED)',
        'general service (incandescent)',
        'ground',
        'home theater and audio',
        'infiltration',
        'kitchen ventilation',
        'lab fridges and freezers',
        'laptop PC',
        'large video boards',
        'laundry',
        'lighting gain',
        'linear fluorescent (LED)',
        'linear fluorescent (T-8)',
        'linear fluorescent (T-12)',
        'medical imaging',
        'microwave',
        'monitors',
        'network equipment',
        'non-road electric vehicles',
        'office UPS',
        'oil_boiler',
        'oil_furnace',
        'oil_water_heater',
        'other',
        'other appliances',
        'other heat gain',
        'people gain',
        'private branch exchanges',
        'pool heaters',
        'pool pumps',
        'portable electric spas',
        'reciprocating_chiller',
        'rechargeables',
        'reflector (CFL)',
        'reflector (LED)',
        'reflector (halogen)',
        'reflector (incandescent)',
        'res_type_central_AC',
        'res_type_gasHP-cool',
        'res_type_gasHP-heat',
        'resistance',
        'resistance heat',
        'roof',
        'rooftop_AC',
        'rooftop_ASHP-cool',
        'rooftop_ASHP-heat',
        'room AC',
        'set top box',
        'screw_chiller',
        'scroll_chiller',
        'security system',
        'security systems',
        'secondary heater',
        'secondary heater (LPG)',
        'secondary heater (coal)',
        'secondary heater (kerosene)',
        'secondary heater (wood)',
        'shredders',
        'small kitchen appliances',
        'smart speakers',
        'smartphones',
        'solar WH',
        'stove (wood)',
        'tablets',
        'telecom systems',
        'ventilation',
        'video game consoles',
        'voice-over-IP telecom',
        'wall',
        'wall-window_room_AC',
        'water services',
        'windows conduction',
        'windows solar',
        'wine coolers'
    ]


YEARS = [str(x) for x in range(2000, 2100)]

DEFINED_VALUES = (
        REGIONS + BUILDING_TYPES + FUEL_TYPES + END_USES +
        SUPPLY_DEMAND + ENERGY_STOCK + TECHNOLOGIES + YEARS
        )


################################################################################
def isfloat(x):
    try:
        float(x)
        return True
    except ValueError:
        return False
    except TypeError:
        return False

################################################################################
def whats_in_a_series(x, print_unknowns = False):
    if isinstance(x, pd.Series):
        rgns = x.isin(REGIONS)
        bdts = x.isin(BUILDING_TYPES)
        flts = x.isin(FUEL_TYPES)
        edus = x.isin(END_USES)
        sd   = x.isin(SUPPLY_DEMAND)
        es   = x.isin(ENERGY_STOCK)
        tech = x.isin(TECHNOLOGIES)
        yrs  = x.isin(YEARS)
        dfnd = x.isin(DEFINED_VALUES)
        flt  = x.apply(isfloat)
        rtn = [
                {"concept" : "region",
                 "no_obs"    : ~rgns.any(),
                 "any_obs"     : rgns.any(),
                 "all_obs"     : rgns.all()
                 },
                {"concept" : "building_type",
                 "no_obs"    : ~bdts.any(),
                 "any_obs"     : bdts.any(),
                 "all_obs"     : bdts.all()
                 },
                {"concept" : "fuel_type",
                 "no_obs"    : ~flts.any(),
                 "any_obs"     : flts.any(),
                 "all_obs"     : flts.all()
                 },
                {"concept" : "end_use",
                 "no_obs"    : ~edus.any(),
                 "any_obs"     : edus.any(),
                 "all_obs"     : edus.all()
                 },
                {"concept" : "supply_demand",
                 "no_obs"    : ~sd.any(),
                 "any_obs"     : sd.any(),
                 "all_obs"     : sd.all()
                 },
                {"concept" : "energy_stock",
                 "no_obs"    : ~es.any(),
                 "any_obs"     : es.any(),
                 "all_obs"     : es.all()
                 },
                {"concept" : "technology",
                 "no_obs"    : ~tech.any(),
                 "any_obs"     : tech.any(),
                 "all_obs"     : tech.all()
                 },
                {"concept" : "year",
                 "no_obs"    : ~yrs.any(),
                 "any_obs"     : yrs.any(),
                 "all_obs"     : yrs.all()
                 },
                {"concept" : "Floating Point Values",
                 "no_obs"    : ~flt.any(),
                 "any_obs"     : flt.any(),
                 "all_obs"     : flt.all()
                 },
                {"concept" : "Defined Values",
                 "no_obs"    : ~dfnd.any(),
                 "any_obs"     : dfnd.any(),
                 "all_obs"     : dfnd.all()
                 },
                {"concept" : "Undefined Values",
                 "no_obs"    : dfnd.all(),
                 "any_obs"     : ~dfnd.all(),
                 "all_obs"     : ~dfnd.any()
                 }
            ]
        rtn = pd.json_normalize(rtn)
        if (~dfnd.all() and print_unknowns):
            print("Undefined values:")
            print(set(x[~dfnd]))
    else:
        print(f"{x} is not a pd.Series")
        rtn = None
    return rtn


