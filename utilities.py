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

    assert bool(data is not None) ^ bool(path is not None)

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

SCENARIOS = ["Max adoption potential", "Technical adoption potential"]

ECMS = [
        '90.1 Com. N-Elec. Env.',
        'Best Com. ASHP, Env., PC (EE+DF–FS)',
        'Best Com. ASHP, Env., PC (EE+DF–FS) CC',
        'Best Com. ASHP, Env., PC (EE+DF–LFL)',
        'Best Com. ASHP, Env., PC (EE+DF–LFL) CC',
        'Best Com. ASHP, Env., PC (EE+DF–RST)',
        'Best Com. ASHP, Env., PC (EE+DF–RST) CC',
        'Best Com. Chillers, Env., PC (EE+DF)',
        'Best Com. Chillers, Ice (EE+DF)',
        'Best Com. Lighting (EE+DF)',
        'Best Com. N-Elec. Env.',
        'Best Com. Plug Loads (EE+DF)',
        'Best Com. RTU, Env., PC (EE+DF)',
        'Best Commercial Gas Storage WH',
        'Best Commercial HPWH',
        'Best Commercial HPWH (FS)',
        'Best Commercial Refrigeration',
        'Best Res. ASHP, Env., PC (EE+DF–FS)',
        'Best Res. ASHP, Env., PC (EE+DF–FS) CC',
        'Best Res. ASHP, Env., PC (EE+DF–LFL)',
        'Best Res. ASHP, Env., PC (EE+DF–LFL) CC',
        'Best Res. ASHP, Env., PC (EE+DF–RST)',
        'Best Res. ASHP, Env., PC (EE+DF–RST) CC',
        'Best Res. CAC, Env., PC (EE+DF)',
        'Best Res. Clothes Dryer (EE+DF)',
        'Best Res. Clothes Washer (EE+DF)',
        'Best Res. Dishwasher (EE+DF)',
        'Best Res. Electronics (EE+DF)',
        'Best Res. HPWH (EE+DF)',
        'Best Res. HPWH (EE+DF–FS)',
        'Best Res. N-Elec. Env.',
        'Best Res. Pool Pump (EE+DF)',
        'Best Residential Gas Dryer',
        'Best Residential Gas Storage WH',
        'Best Residential LED Lighting',
        'Best Residential Oil WH',
        'Best Residential Refrigerator',
        'Com. Cooking (FS)',
        'Com. Fossil Heating, Ref. Case',
        'Com. Fossil Heating, Ref. Case (2030)',
        'Com. Fossil Heating, Ref. Case (2035)',
        'Commercial Lighting, 90.1 c. 2019',
        'Commercial Lighting, Ref. Case',
        'Commercial Plug Loads, Tier 1 APS',
        'ENERGY STAR Com. ASHP (FS) + Env.',
        'ENERGY STAR Com. ASHP (FS) + Env. CC',
        'ENERGY STAR Com. ASHP (LFL) + Env.',
        'ENERGY STAR Com. ASHP (LFL) + Env. CC',
        'ENERGY STAR Com. ASHP (RST) + Env.',
        'ENERGY STAR Com. ASHP (RST) + Env. CC',
        'ENERGY STAR Com. RTU + Env.',
        'ENERGY STAR Commercial Gas WH v. 2.0',
        'ENERGY STAR Electric Dryers',
        'ENERGY STAR Electronics',
        'ENERGY STAR Gas Dryers v. 1.1',
        'ENERGY STAR Gas Storage WH v. 4.0',
        'ENERGY STAR Refrigerator',
        'ENERGY STAR Res. ASHP (FS) + Env.',
        'ENERGY STAR Res. ASHP (FS) + Env. CC',
        'ENERGY STAR Res. ASHP (LFL) + Env.',
        'ENERGY STAR Res. ASHP (LFL) + Env. CC',
        'ENERGY STAR Res. ASHP (RST) + Env.',
        'ENERGY STAR Res. ASHP (RST) + Env. CC',
        'ENERGY STAR Res. CAC + Env.',
        'ENERGY STAR Res. HPWH',
        'ENERGY STAR Res. HPWH (FS)',
        'ESTAR IECC Res. N-Elec. Env.',
        'Prosp. Com. ASHP (FS) + Env. + Ctls.',
        'Prosp. Com. ASHP (FS) + Env. + Ctls. CC',
        'Prosp. Com. ASHP (LFL) + Env. + Ctls.',
        'Prosp. Com. ASHP (LFL) + Env. + Ctls. CC',
        'Prosp. Com. ASHP (RST) + Env. + Ctls.',
        'Prosp. Com. ASHP (RST) + Env. + Ctls. CC',
        'Prosp. Com. Chillers + Env. + Ctls.',
        'Prosp. Com. N-Elec. Env.',
        'Prosp. Com. N-Elec. Env. + Ctls.',
        'Prosp. Com. RTU + Env. + Ctls.',
        'Prosp. Res. ASHP (FS) + Env. + Ctls.',
        'Prosp. Res. ASHP (FS) + Env. + Ctls. CC',
        'Prosp. Res. ASHP (LFL) + Env. + Ctls.',
        'Prosp. Res. ASHP (LFL) + Env. + Ctls. CC',
        'Prosp. Res. ASHP (RST) + Env. + Ctls.',
        'Prosp. Res. ASHP (RST) + Env. + Ctls. CC',
        'Prosp. Res. CAC + Env. + Ctls.',
        'Prosp. Res. N-Elec. Env.',
        'Prosp. Res. N-Elec. Env. + Ctls.',
        'Prospective Com. Ctls. (Lighting)',
        'Prospective Commercial HPWH',
        'Prospective Commercial HPWH (FS)',
        'Prospective Commercial Refrigeration',
        'Prospective Residential Ctls. (Lighting)',
        'Prospective Residential Dryer',
        'Prospective Residential HPWH',
        'Prospective Residential HPWH (FS)',
        'Prospective Residential Refrigeration',
        'Ref. Case Clothes Washer',
        'Ref. Case Com. ASHP (FS)',
        'Ref. Case Com. ASHP (FS) CC',
        'Ref. Case Com. ASHP (LFL)',
        'Ref. Case Com. ASHP (LFL) CC',
        'Ref. Case Com. ASHP (RST)',
        'Ref. Case Com. ASHP (RST) CC',
        'Ref. Case Com. Chillers',
        'Ref. Case Com. Elec. HVAC (FS)',
        'Ref. Case Com. Elec. HVAC (FS) CC',
        'Ref. Case Com. Elec. WH (FS)',
        'Ref. Case Com. Plug Loads',
        'Ref. Case Com. RTU',
        'Ref. Case Com. Refrigeration',
        'Ref. Case Commercial Gas WH v. 2.0',
        'Ref. Case Electric Dryers',
        'Ref. Case Electronics',
        'Ref. Case Gas Dryers v. 1.1',
        'Ref. Case Gas Storage WH v. 4.0',
        'Ref. Case LED Bulbs',
        'Ref. Case Other Res. Heating (FS)',
        'Ref. Case Other Res. Heating (FS) CC',
        'Ref. Case Refrigerator',
        'Ref. Case Res. ASHP (FS)',
        'Ref. Case Res. ASHP (FS) CC',
        'Ref. Case Res. ASHP (LFL)',
        'Ref. Case Res. ASHP (LFL) CC',
        'Ref. Case Res. ASHP (RST)',
        'Ref. Case Res. ASHP (RST) CC',
        'Ref. Case Res. CAC',
        'Ref. Case Res. Dishwasher',
        'Ref. Case Res. Elec. HVAC (FS)',
        'Ref. Case Res. Elec. HVAC (FS) CC',
        'Ref. Case Res. Elec. WH (FS)',
        'Ref. Case Res. HPWH',
        'Ref. Case Res. Pool Pump',
        'Res. Cooking (FS)',
        'Res. Fossil Heating, Ref. Case',
        'Res. Fossil Heating, Ref. Case (2030)',
        'Res. Fossil Heating, Ref. Case (2035)',
        'Residential Oil WH, 90.1 c. 2019',
        'Residential Oil WH, Ref. Case'
        ]

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
        'Retail', 'Assembly/Other', 'Warehouses', 'Multi Family Homes',
        'Education', 'Large Offices', 'Single Family Homes',
        'Small/Medium Offices', 'Hospitality', 'Hospitals',
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
        'electricity', 'Electric',
        'natural gas', 'Natural Gas', 'Propane',
        'distillate', 'Distillate/Other', 'Biomass',
        'other fuel'
        ]

END_USES = [
        'Water Heating',
        'Other',
        'Cooking',
        'Refrigeration',
        'Ventilation',
        'Heating (Equip.)',
        'Cooling (Equip.)',
        'Computers and Electronics',
        'Lighting',
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

OUTCOME_METRICS = [
        'Avoided CO\u2082 Emissions (MMTons)', #'Avoided CO₂ Emissions (MMTons)',
        'Baseline CO\u2082 Cost (USD)', #'Baseline CO₂ Cost (USD)',
        'Baseline CO\u2082 Emissions (MMTons)', #'Baseline CO₂ Emissions (MMTons)',
        'Baseline Energy Use (MMBtu)',
        'Baseline Energy Cost (USD)',
        'CO\u2082 Cost Savings (USD)', #'CO₂ Cost Savings (USD)',
        'CO\u2082 Emissions (MMTons)', # CO₂ Emissions (MMTons)
        'Cost of Conserved CO\u2082 ($/MTon CO\u2082 avoided)', #'Cost of Conserved CO₂ ($/MTon CO₂ avoided)'
        'Cost of Conserved Energy ($/MMBtu saved)',
        'Efficient CO\u2082 Cost (USD)', #'Efficient CO₂ Cost (USD)',
        'Efficient CO\u2082 Emissions (MMTons)', #'Efficient CO₂ Emissions (MMTons)',
        'Efficient Energy Cost (USD)',
        'Efficient Energy Use (MMBtu)',
        'Energy (MMBtu)',
        'Energy Cost (USD)',
        'Energy Cost Savings (USD)',
        'Energy Savings (MMBtu)',
        'IRR (%)',
        'Payback (years)'
        ]

YEARS = [str(x) for x in range(2000, 2100)]

DEFINED_VALUES = (
        ECMS + SCENARIOS +
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


