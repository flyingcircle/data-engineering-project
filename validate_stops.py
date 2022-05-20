import logging
import pandas as pd
import numpy as np

def validate(df: pd.DataFrame):
  logging.basicConfig(filename='stop_consumer_validations.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
  #Assertion 1 (existence) All records have a service key
  if (df['service_key'] == '').any():
    df = df[df['service_key'].isnull() == False]
    logging.info("Assertion 1 failed: some records do not have a service key")
 
  #Assertion 2 (limit) direction is 0 or 1
  if not (df['direction'].between(0,1).all()):
    logging.info("Assertion 2 failed: some records have a direction that is not OUT or BACK")
    df = df[df['direction'].between(0,1)]

  #Assertion 3 (limit) service_key is W or S
  if not (df['service_key'].isin(['W', 'S', 'U']).all()):
    logging.info('Assertion 3 failed: some records have an invalid service key')
    df = df[df['service_key'].isin(['W', 'S', 'U'])]

  #Assertion 4 (existence) Every record has a trip id
  if (df['trip_id'].isnull().values.any()):
    logging.info("Assertion 4 failed: some records do not have a trip id")
    df = df[df['trip_id'].isnull() == False]

  #Assertion 5 (inter record) Every trip id has at most 1 route
  trip_route_count_uniques = df.groupby('trip_id')['route_num'].nunique()
  if (trip_route_count_uniques.max() > 1):
    td = trip_route_count_uniques.to_dict()
    x = [x for (x, y) in td.items() if y == 1]
    df = df[df['trip_id'].isin(x)]
    logging.info("Assertion 5 failed: some records have more than one route for a single trip id")

  #Assertion 6 (intra record) The service key is the same for every record
  if not ((df['service_key'] == 'W').all() or (df['service_key'] == 'S').all() or (df['service_key'] == 'U').all()):
    logging.info("Assertion 6 failed. Some records have differing service keys for same date")
    counts = df.groupby('service_key').count().reset_index()
    key = counts['service_key'].max()
    df = df[df['service_key'] == key]
    
  return df

def fix_types(df: pd.DataFrame):
  from enum import Enum

  df['trip_id'] = pd.to_numeric(df['trip_id'])
  df['route_num'] = pd.to_numeric(df['route_num'])
  df['direction'] = pd.to_numeric(df['directions'])

  class Service(Enum):
    Weekday = 0
    Saturday = 1
    Sunday = 2

  services = [Service.Weekday if x == 'W' else Service.Saturday if x == 'S' else Service.Sunday for x in df['service_key']]
  df['service_key'] = services
  return df

