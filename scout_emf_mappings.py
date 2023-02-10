import pandas as pd
from scout_concepts import ScoutConcepts
scout_concpets = ScoutConcepts()

class ScoutEMFMappings():
    def __init__(self):
        # Unit conversions.
        self.MMBtu_to_EJ           = 1.05505585262e-9
        self.EJ_to_quad            = 0.9478
        self.pound_to_mt           = 0.000453592
        self.EJ_to_twh             = 277.778
        self.EJ_to_mt_co2_propane  = self.EJ_to_quad * 62.88
        self.EJ_to_mt_co2_kerosene = self.EJ_to_quad * 73.38
        self.EJ_to_mt_co2_gas      = self.EJ_to_quad * 53.056
        self.EJ_to_mt_co2_oil      = self.EJ_to_quad * 74.14
        self.EJ_to_mt_co2_bio      = self.EJ_to_quad * 96.88

        self.emf_base_strings = pd.DataFrame(
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

        self.emf_end_uses = pd.DataFrame([
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

        self.emf_direct_indirect_fuel = pd.DataFrame([
            {'scout_fuel_type': 'Natural Gas',     'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'natural gas',     'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'Distillate/Other','emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'distillate',      'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'Biomass',         'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'Propane',         'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'Electric',        'emf_direct_indirect_fuel': 'Indirect'},
            {'scout_fuel_type': 'electricity',     'emf_direct_indirect_fuel': 'Indirect'},
            {'scout_fuel_type': 'Non-Electric',    'emf_direct_indirect_fuel': 'Direct'},
            {'scout_fuel_type': 'other fuel',      'emf_direct_indirect_fuel': 'Direct'}
            ])

        self.non_other_fuel_type = pd.DataFrame([
            {
                'scout_non_other_fuel_type': 'Natural Gas',
                'emf_fuel_type': 'Gas',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_gas
            },
            {
                'scout_non_other_fuel_type': 'natural gas',
                'emf_fuel_type': 'Gas',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_gas
            },
            {
                'scout_non_other_fuel_type': 'Propane',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_propane
            },
            {
                'scout_non_other_fuel_type': 'Distillate/Other',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_non_other_fuel_type': 'distillate',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_non_other_fuel_type': 'Biomass',
                'emf_fuel_type': 'Biomass Solids',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_non_other_fuel_type': 'Electric',
                'emf_fuel_type': 'Electricity',
                'EJ_to_mt_CO2': self.EJ_to_twh
            },
            {
                'scout_non_other_fuel_type': 'Electricity',
                'emf_fuel_type': 'Electricity',
                'EJ_to_mt_CO2': self.EJ_to_twh
            },
            {
                'scout_non_other_fuel_type': 'electricity',
                'emf_fuel_type': 'Electricity',
                'EJ_to_mt_CO2': self.EJ_to_twh
            }
            ])

        self.other_fuel_type = pd.DataFrame([
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'heating',
                'scout_technology': 'furnace (LPG)',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_propane
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'heating',
                'scout_technology': 'furnace (kerosene)',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_kerosene
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'heating',
                'scout_technology': 'stove (wood)',
                'emf_fuel_type': 'Biomass Solids',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_bio
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'secondary heating',
                'scout_technology': 'secondary heater (wood)',
                'emf_fuel_type': 'Biomass Solids',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_bio
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'secondary heating',
                'scout_technology': 'secondary heater (coal)',
                'emf_fuel_type': 'Biomass Solids',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_bio
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'secondary heating',
                'scout_technology': 'secondary heater (kerosene)',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_kerosene
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'secondary heating',
                'scout_technology': 'secondary heater (LPG)',
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_propane
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'water heating',
                'scout_technology': pd.NA,
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'cooking',
                'scout_technology': pd.NA,
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'drying',
                'scout_technology': pd.NA,
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            },
            {
                'scout_other_fuel_type': 'other fuel',
                'scout_end_use': 'other',
                'scout_technology': pd.NA,
                'emf_fuel_type': 'Oil',
                'EJ_to_mt_CO2': self.EJ_to_mt_co2_oil
            }
            ])

