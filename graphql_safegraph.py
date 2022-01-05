# %%
# https://pypi.org/project/safegraphQL/
# import sys
# !{sys.executable} -m pip install safegraphQL
# %%
# import sys
# !{sys.executable} -m pip install --pre gql
# %%
# https://docs.safegraph.com/reference#places-api-overview-new
# https://stackoverflow.com/questions/56856005/how-to-set-environment-variable-in-databricks/56863551
import pandas as pd
import json

import os
from dotenv import load_dotenv

load_dotenv()
sfkey = os.environ.get("SAFEGRAPH_KEY")

# %%
url = 'https://api.safegraph.com/v2/graphql'
query = """
query {
  lookup(placekey: "225-222@5vg-7gs-t9z"){
    placekey
    safegraph_core {
      location_name
      region
      postal_code
    }
  }
}
"""

query2 = """query {
  search(filter: { 
    naics_code: 813110,
    address: {
      region: "UT"
    }
  }){
    places {
      results(first: 500 after: "") {
        pageInfo { hasNextPage, endCursor}
        edges {
          node {
            safegraph_weekly_patterns (date: "2021-07-12") {
              placekey
              parent_placekey
              location_name
              street_address
              city
              region
              postal_code
              iso_country_code
              date_range_start
              date_range_end
              raw_visit_counts
              raw_visitor_counts
              poi_cbg
              visitor_home_cbgs
              visitor_home_aggregation
              visitor_daytime_cbgs
              visitor_country_of_origin
              distance_from_home
              median_dwell
              bucketed_dwell_times
              related_same_day_brand
              related_same_week_brand
            }
          }
        }
      }
    }
  }
}
"""

# %%
# Using the requests package
import requests
r = requests.post(
    url,
    json={'query': query},
    headers = {'Content-Type': 'application/json', 'apikey':sfkey})

# %%
print(r.status_code)
print(r.text)
json_data = json.loads(r.text)
df_data = json_data['data']['lookup']
print(df_data)

# %%
pract = df_data.copy()

pd.json_normalize(pract)
# %%

# https://gql.readthedocs.io/en/v3.0.0a6/
# https://github.com/graphql-python/gql
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

# Select your transport with a defined url endpoint
transport = RequestsHTTPTransport(
    url=url,
    verify=True,
    retries=3,
    headers={'Content-Type': 'application/json', 'apikey': sfkey})

client = Client(transport=transport, fetch_schema_from_transport=True)


# %%
results = client.execute(gql(query2))


# %%
edges = results['search']['places']['results']['edges']
resultsNorm = [dat.pop('node') for dat in edges]
resultsNorm = [dat.pop('safegraph_weekly_patterns') for dat in resultsNorm]

dat = pd.json_normalize(resultsNorm)

# %%
# https://pypi.org/project/safegraphQL/
# https://github.com/echong-SG/API-python-client-MKilic
import safegraphql.client as sgql
sgql_client = sgql.HTTP_Client(apikey = sfkey)

# %%
pks = [
    'zzw-222@8fy-fjg-b8v', # Disney World 
    'zzw-222@5z6-3h9-tsq'  # LAX
]
cols = [
    'location_name',
    'street_address',
    'city',
    'region',
    'postal_code',
    'iso_country_code'
]

sgql_client.lookup(product = 'core', placekeys = pks, columns = cols)
# %%
sgql_client.lookup(product = 'core', placekeys = pks, columns = "*")

# %%
geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = '*')
patterns = sgql_client.lookup(product = 'monthly_patterns', placekeys = pks, columns = '*')

# %%
watterns = sgql_client.lookup(product = 'weekly_patterns', placekeys = pk, columns = '*')

# %%
## weekly patterns
dates = ['2019-06-15', '2019-06-16', '2021-05-23', '2018-10-23']

sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pks, 
    date = dates, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)
# %%
dates = {'date_range_start': '2019-04-10', 'date_range_end': '2019-06-05'}

watterns = sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pks, 
    date = dates, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)

core = sgql_client.lookup(product = 'core', placekeys = pks, columns = ['placekey', 'location_name', 'naics_code', 'top_category', 'sub_category'])
geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = ['placekey', 'polygon_class', 'enclosed'])

# %%
merged = sgql_client.sg_merge(datasets = [core, geo, watterns])
# %%
# look-up by name
# https://github.com/echong-SG/API-python-client-MKilic#lookup_by_name