import json
import pandas as pd
import numpy as np
from Scout_Concepts import ScoutConcepts

scout_concepts = ScoutConcepts()

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
        rgns = x.isin(scout_concepts.regions)
        bdts = x.isin(scout_concepts.building_types)
        flts = x.isin(scout_concepts.fuel_types)
        edus = x.isin(scout_concepts.end_uses)
        sd   = x.isin(scout_concepts.supply_demand)
        es   = x.isin(scout_concepts.energy_stock)
        tech = x.isin(scout_concepts.technologies)
        yrs  = x.isin(scout_concepts.years)
        dfnd = x.isin(scout_concepts.defined_values)
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


################################################################################
def five_number_summary(x, ignorenan = False):
    if ignorenan:
        qs = np.nanpercentile(x, [25, 50, 75])
        mn = np.nanmin(x)
        mx = np.nanmax(x)
    else:
        qs = np.percentile(x, [25, 50, 75])
        mn = np.min(x)
        mx = np.max(x)
    return {"Min" : mn, "Q1" : qs[0], "Median" : qs[1], "Q3" : qs[2], "Max" : mx}

def fns(x, ignorenan = False):
    y = five_number_summary(x, ignorenan)
    for k,v in y.items():
        print((k+":").ljust(8) + f"{v:.4e}")
