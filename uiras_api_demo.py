## pyspark script for reading swimming location data from uiras open api

from urllib.request import urlopen
from flatjson import flatten_json
from pyspark.sql import DataFrame as SDF

url = "https://iot.fvh.fi/opendata/uiras/uiras-meta.json"
jsonData = urlopen(url).read().decode('utf-8')

import json

# the json is very poorly formatted for reading with spark,
# reformulate and infer schema
# TODO: how to do this part in spark?
d = json.loads(jsonData)
keys = list(d.keys())
for key in list(d.keys()):
    d[key]['id'] = key

# infer schema
schema_sample =  json.dumps(d[list(d.keys())[0]])
schema = spark.read.json(sc.parallelize([schema_sample])).schema

# read and add to dataframe element by element
df_list = []
for key in list(d.keys()):
    jsonElement = json.dumps(d[key])
    # read with spark and 
    df_list.append(spark.read.json(sc.parallelize([jsonElement])))


from functools import reduce

df = reduce(lambda df1, df2: SDF.unionByName(df1, df2, allowMissingColumns = True), df_list)

df_flat = flatten_json(df)
