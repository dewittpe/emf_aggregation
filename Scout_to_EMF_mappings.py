import pandas as pd

# Unit conversions.
MMBtu_to_EJ           = 1.05505585262e-9
EJ_to_quad            = 0.9478
pound_to_mt           = 0.000453592
EJ_to_twh             = 277.778
EJ_to_mt_co2_propane  = EJ_to_quad * 62.88
EJ_to_mt_co2_kerosene = EJ_to_quad * 73.38
EJ_to_mt_co2_gas      = EJ_to_quad * 53.056
EJ_to_mt_co2_oil      = EJ_to_quad * 74.14
EJ_to_mt_co2_bio      = EJ_to_quad * 96.88

EMF_BASE_STRINGS = pd.DataFrame(
    [
        {
            'outcome_metric' : 'Avoided CO\u2082 Emissions (MMTons)',
            'emf_base_string': '*Emissions|CO2|Energy|Demand|Buildings'
        },
        {
            'outcome_metric' : 'Energy Savings (MMBtu)',
            'emf_base_string': '*Final Energy|Buildings'
        }
    ]
    )

BUILDING_CLASSES = pd.DataFrame(
        [
            {
                'building_class0': 'Commercial (New)',
                'structure_type': 'New',
                'building_class': 'Commercial'
             },
            {
                'building_class0': 'Commercial (Existing)',
                'structure_type': 'Existing',
                'building_class': 'Commercial'
             },
            {
                'building_class0': 'Residential (New)',
                'structure_type': 'New',
                'building_class': 'Residential'
             },
            {
                'building_class0': 'Residential (Existing)',
                'structure_type': 'Existing',
                'building_class': 'Residential'
             }
        ]
        )

BUILDING_TYPE_CLASS = pd.DataFrame(
        [
            {'building_type' : 'Assembly/Other',      'building_class': 'Commercial'},
            {'building_type': 'Education',            'building_class': 'Commercial'},
            {'building_type': 'Hospitality',          'building_class': 'Commercial'},
            {'building_type': 'Hospitals',            'building_class': 'Commercial'},
            {'building_type': 'Large Offices',        'building_class': 'Commercial'},
            {'building_type': 'Multi Family Homes',   'building_class': 'Residential'},
            {'building_type': 'Retail',               'building_class': 'Commercial'},
            {'building_type': 'Single Family Homes',  'building_class': 'Residential'},
            {'building_type': 'Small/Medium Offices', 'building_class': 'Commercial'},
            {'building_type': 'Warehouse',            'building_class': 'Commercial'},
            {'building_type': 'assembly',           'building_class': 'Commercial'},
            {'building_type': 'education',          'building_class': 'Commercial'},
            {'building_type': 'food sales',         'building_class': 'Commercial'},
            {'building_type': 'food service',       'building_class': 'Commercial'},
            {'building_type': 'health care',        'building_class': 'Commercial'},
            {'building_type': 'lodging',            'building_class': 'Commercial'},
            {'building_type': 'large office',       'building_class': 'Commercial'},
            {'building_type': 'small office',       'building_class': 'Commercial'},
            {'building_type': 'mercantile/service', 'building_class': 'Commercial'},
            {'building_type': 'warehouse',          'building_class': 'Commercial'},
            {'building_type': 'other',              'building_class': 'Commercial'},
            {'building_type': 'unspecified',        'building_class': 'Commercial'},
            {'building_type': 'single family home', 'building_class': 'Residential'},
            {'building_type': 'multi family home',  'building_class': 'Residential'},
            {'building_type': 'mobile home',        'building_class': 'Residential'},
            {'building_type': 'Mobile Homes',       'building_class': 'Residential'}
        ])

EMF_END_USES = pd.DataFrame([
    {'scout_end_use': 'Cooking',                   'emf_end_use': 'Appliances'},
    {'scout_end_use': 'Cooling (Env.)',            'emf_end_use': 'unmapped'},
    {'scout_end_use': 'Cooling (Equip.)',          'emf_end_use': 'Cooling'},
    {'scout_end_use': 'Computers and Electronics', 'emf_end_use': 'Other'},
    {'scout_end_use': 'Heating (Env.)',            'emf_end_use': 'unmapped'},
    {'scout_end_use': 'Heating (Equip.)',          'emf_end_use': 'Heating'},
    {'scout_end_use': 'Lighting',                  'emf_end_use': 'Lighting'},
    {'scout_end_use': 'Other',                     'emf_end_use': 'Other'},
    {'scout_end_use': 'Refrigeration',             'emf_end_use': 'Appliances'},
    {'scout_end_use': 'Ventilation',               'emf_end_use': 'Heating'},
    {'scout_end_use': 'Water Heating',             'emf_end_use': 'Appliances'},
    {'scout_end_use': 'ceiling fan',               'emf_end_use': 'Appliances'},
    {'scout_end_use': 'cooking',                   'emf_end_use': 'Appliances'},
    {'scout_end_use': 'cooling',                   'emf_end_use': 'Cooling'},
    {'scout_end_use': 'computers',                 'emf_end_use': 'Other'},
    {'scout_end_use': 'drying',                    'emf_end_use': 'Appliances'},
    {'scout_end_use': 'fans and pumps',            'emf_end_use': 'Heating'},
    {'scout_end_use': 'heating',                   'emf_end_use': 'Heating'},
    {'scout_end_use': 'lighting',                  'emf_end_use': 'Lighting'},
    {'scout_end_use': 'MELs',                      'emf_end_use': 'Other'},
    {'scout_end_use': 'non-PC office equipment',   'emf_end_use': 'Other'},
    {'scout_end_use': 'other',                     'emf_end_use': 'Other'},
    {'scout_end_use': 'onsite generation',         'emf_end_use': 'unmapped'},
    {'scout_end_use': 'PCs',                       'emf_end_use': 'Other'},
    {'scout_end_use': 'refrigeration',             'emf_end_use': 'Appliances'},
    {'scout_end_use': 'secondary heating',         'emf_end_use': 'Heating'},
    {'scout_end_use': 'TVs',                       'emf_end_use': 'Other'},
    {'scout_end_use': 'unspecified',               'emf_end_use': 'Other'},
    {'scout_end_use': 'ventilation',               'emf_end_use': 'Heating'},
    {'scout_end_use': 'water heating',             'emf_end_use': 'Appliances'}
    ])

EMF_DIRECT_INDIRECT_FUEL = pd.DataFrame([
    {"scout_fuel_type": "Natural Gas",     "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "natural gas",     "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "Distillate/Other","emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "distillate",      "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "Biomass",         "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "Propane",         "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "Electric",        "emf_direct_indirect_fuel": "Indirect"},
    {"scout_fuel_type": "electricity",     "emf_direct_indirect_fuel": "Indirect"},
    {"scout_fuel_type": "Non-Electric",    "emf_direct_indirect_fuel": "Direct"},
    {"scout_fuel_type": "other fuel",      "emf_direct_indirect_fuel": "Direct"}
    ])

EMF_FUEL_TYPE = pd.DataFrame([
    {'scout_split_fuel': "other fuel", "scout_end_use": "heating",            'scout_technology': "furnace (LPG)",                "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_propane},
    {'scout_split_fuel': "other fuel", "scout_end_use": "heating",            'scout_technology': "furnace (kerosene)",           "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_kerosene},
    {'scout_split_fuel': "other fuel", "scout_end_use": "heating",            'scout_technology': "stove (wood)",                 "emf_fuel_type": "Biomass Solids",  "EJ_to_mt_CO2": EJ_to_mt_co2_bio},
    {'scout_split_fuel': "other fuel", "scout_end_use": "secondary heating",  'scout_technology': "secondary heater (wood)",      "emf_fuel_type": "Biomass Solids",  "EJ_to_mt_CO2": EJ_to_mt_co2_bio},
    {'scout_split_fuel': "other fuel", "scout_end_use": "secondary heating",  'scout_technology': "secondary heater (coal)",      "emf_fuel_type": "Biomass Solids",  "EJ_to_mt_CO2": EJ_to_mt_co2_bio},
    {'scout_split_fuel': "other fuel", "scout_end_use": "secondary heating",  'scout_technology': "secondary heater (kerosene)",  "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_kerosene},
    {'scout_split_fuel': "other fuel", "scout_end_use": "secondary heating",  'scout_technology': "secondary heater (LPG)",       "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_propane},
    {'scout_split_fuel': "other fuel", "scout_end_use": "water heating",      'scout_technology': pd.NA,                          "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_oil},
    {'scout_split_fuel': "other fuel", "scout_end_use": "cooking",            'scout_technology': pd.NA,                          "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_oil},
    {'scout_split_fuel': "other fuel", "scout_end_use": "drying",             'scout_technology': pd.NA,                          "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_oil},
    {'scout_split_fuel': "other fuel", "scout_end_use": "other",              'scout_technology': pd.NA,                          "emf_fuel_type": "Oil",             "EJ_to_mt_CO2": EJ_to_mt_co2_oil}
    ])



def main():
    # do nothing
    return True

if __name__ == '__main__':
    main()
