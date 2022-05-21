import pandas as pd
import numpy as np
import json

from validate_stops import validate, fix_types
from load_data_to_postgres import load_data

if __name__ == '__main__':
trip_ids = [170969190, 170969190, 170969340, 170969396, 170969469, 170969469]
route_nums = [164, 164, 134, 105, 6, 157]
directions = [0, 1, 1, 1, 1, 0]
service_keys = ['U', 'U', 'U', 'U', 'S', 'S']
trips = pd.DataFrame({
  "trip_id": trip_ids,
  "route_num": route_nums,
  "direction": directions,
  "service_key": service_keys, 
})

validated_trips = validate(trips)
fixed_trips = fix_types(validated_trips)

print(validated_trips)
print(fixed_trips)
print(fixed_trips.dtypes)
load_data(fixed_trips, 'test_stop')
