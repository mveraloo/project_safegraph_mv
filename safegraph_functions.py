# https://towardsdatascience.com/cleaning-and-extracting-json-from-pandas-dataframes-f0c15f93cb38
# https://packaging.python.org/tutorials/packaging-projects/
import pandas as pd
import json
import re

def jsonloads(x):
    if pd.isna(x):
        return None
    else:
        return json.loads(x)

def createlist(x):
    try:
        return x.str.strip('][').str.split(',')
    except:
        return None

def rangenumbers(x):
    if x.size == 1:
        return 0
    else:
        return range(1, x.size + 1)

def expand_json(var, dat):

    rowid = dat.placekey

    parsedat = dat[var]
    loadsdat = parsedat.apply(jsonloads)
    df_wide = pd.json_normalize(loadsdat)

    # clean up store names so they work as column names
    col_names = df_wide.columns
    col_names = [re.sub(r'[^\w\s]','', x) for x in col_names] # remove non-alphanumeric characters
    col_names = [str(col).lower().replace(" ", "_") for col in col_names] # replace spaces with dashes
    col_names_long = [var + '-' + col for col in col_names] 
    
    # rename the columns
    df_wide.columns = col_names_long # add variable name to column names

    df_wide = df_wide.assign(placekey = rowid)

    out = df_wide.loc[:, ["placekey"] + col_names_long]

    return out

def expand_list(var, dat):

    dat_expand = (dat
        .assign(lvar = createlist(dat[var]))
        .filter(["placekey", "lvar"])
        .explode("lvar")
        .reset_index(drop=True)
        .rename(columns={"lvar":var})
    )

    dat_label = (dat_expand
        .groupby('placekey')
        .transform(lambda x: rangenumbers(x))
        .reset_index(drop=True)
    )
    
    if var.find("hour") !=-1:
        dat_label.columns = ['hour']
    elif var.find("day") !=-1:
        dat_label.columns = ['day']
    else :
        dat_label.columns = ['sequence']

    out = pd.concat([dat_expand, dat_label], axis=1)
    out[var] = out[var].astype(float)
    return out
