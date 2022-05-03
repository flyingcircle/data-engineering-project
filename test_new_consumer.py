from new_consumer import *
import pandas as pd
import json


f = open("sample.json")
j = json.load(f)
f.close()
df = pd.DataFrame.from_records(j)
df = fix_types(df)
validate(df)
BreadCrumb, Trip = reshape(df)
load_data(BreadCrumb, "BreadCrumb")
load_data(Trip, "Trip")

