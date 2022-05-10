#! /home/production/bus-py-data/bin/python
from confluent_kafka import Consumer
from datetime import timedelta
import logging
import pandas as pd
import numpy as np
import json
import ccloud_lib

from load_data_to_postgres import load_data

logging.basicConfig(filename='/home/production/logs/consumer.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

def validate(df: pd.DataFrame):
  # Assertion 1 (existence): All records have a lat/lon
  if (df['GPS_LATITUDE'] == '').any() or (df['GPS_LONGITUDE'] == '').any() :
    logging.error('Assertion 1 failed: some records are missing lat/lon values')
    df = df[df['GPS_LATITUDE'] != np.nan]
    df = df[df['GPS_LONGITUDE'] != np.nan]
  
  # Assertion 2 (inter-record): If bus has speed, it has direction
  if not (df['DIRECTION'].all() == df['VELOCITY'].all()):
    logging.error('Assertion 2 failed: some records have speed but not direction/direction but not speed')
    df = df[(df['VELOCITY'] == np.nan) == (df['DIRECTION'] == np.nan)]

  # Assertion 3 (limit): bus velocity must be beneath 80mph
  if not (df['VELOCITY'] < 80).all():
    logging.error('Assertion 3 failed: velocity exceeded 80mph')
    df = df[df['VELOCITY'] < 80]

  # Assertion 4 (summary): Meter count should always be ascending for a trip
  trips = df.groupby('EVENT_NO_TRIP')
  for trip, group in trips:
    if (not group['METERS'].is_monotonic_increasing):
      logging.error("Assertion 4 failed: meters weren't ascending")
      df = df[df['EVENT_NO_TRIP' != trip]]
                
  # Assertion 5 (limit): Bus direction should be between 0 and 359
  if (df['DIRECTION'] < 0).any() or (df['DIRECTION'] > 359).any():
    logging.error('Assertion 5 failed: bus direction was out of bounds')
    df = df[df['DIRECTION'] > 0]
    df = df[df['DIRECTION'] < 360]
    
  # Assertion 6 (existence): No records should have repetition
  if (df.duplicated().any()):
    logging.error('Assertion 6 failed: some records are repeated')
    #if any records are repeated drop the duplicate values keeping the top
    df = df.drop_duplicates()
    
  # Assertion 7 (statistical): Satellite count for each trip is greater than 8 in average
  stat = df.groupby('EVENT_NO_TRIP').sum()/df.groupby('EVENT_NO_TRIP').count()
  if not (stat['GPS_SATELLITES'] > 8).all():
    logging.error('Assertion 7 failed: satellite count for each trip is less than 8 in average')
  
  # Assertion 8 (summary): The number of meters a vehicle travel must not exceed some reasonable limit (300,000?)
  min_meter_value = df.groupby('EVENT_NO_TRIP').min()
  max_meter_value = df.groupby('EVENT_NO_TRIP').max()
  diff_value = max_meter_value['METERS'] - min_meter_value['METERS']
  if (diff_value.max() > 300000):
    logging.error('Assertion 8 failed: some vehicles exceeds the expected value of 300000 meters of travel')

  # Assertion 9 (intra-record): Single trip index should have single vehicle ID 
  trip_vehicle = df.groupby('EVENT_NO_TRIP').VEHICLE_ID.unique()
  trip_vehicle_count = trip_vehicle.groupby(['EVENT_NO_TRIP']).count()
  if (trip_vehicle_count.max() > 1):
    logging.error('Assertion 9 failed: for a single trip more than one associated vehicle_id found')
    
  # Assertion 10 (limit): ACT_TIME should be >= 0
  if not (df['ACT_TIME'].min() >= timedelta(0)):
    logging.error('Assertion 10 failed: clock time is invalid')
    df = df[df['ACT_TIME'] >= timedelta(0)]
    
  return df

def fix_types(df: pd.DataFrame):
  df['GPS_LATITUDE'] = pd.to_numeric(df['GPS_LATITUDE'])
  df['GPS_LONGITUDE'] = pd.to_numeric(df['GPS_LONGITUDE'])
  df['DIRECTION'] = pd.to_numeric(df['DIRECTION'])
  df['EVENT_NO_TRIP'] = pd.to_numeric(df['EVENT_NO_TRIP'])
  df['EVENT_NO_STOP'] = pd.to_numeric(df['EVENT_NO_STOP'])
  df['VEHICLE_ID'] = pd.to_numeric(df['VEHICLE_ID'])
  df['METERS'] = pd.to_numeric(df['METERS'])
  df['VELOCITY'] = df['VELOCITY'].replace(to_replace='', value='0')
  df['VELOCITY'] = pd.to_numeric(df['VELOCITY']).apply(lambda x: x*2.236936)
  df['ACT_TIME'] = pd.to_numeric(df['ACT_TIME'])
  df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='s')
  df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'])
  df['OPD_DATE'] = df['OPD_DATE'].dt.tz_localize(tz='US/Pacific')
  
  return df

def reshape(df: pd.DataFrame):

  def isWeekday(day: str):
    return day not in ["Sunday", "Saturday"]

  timestamp = df['OPD_DATE'] + df['ACT_TIME']
  weekday = timestamp.dt.day_name().apply(lambda x: "Weekday" if isWeekday(x) else x)
  # The first schema required for part B, "BreadCrumb"
  BreadCrumb = df[[
    'GPS_LATITUDE', 
    'GPS_LONGITUDE', 
    'DIRECTION', 
    'VELOCITY',
    'EVENT_NO_TRIP'
  ]]
  BreadCrumb = pd.concat([timestamp, BreadCrumb], axis=1)
  BreadCrumb.columns = ['tstamp','latitude','longitude','direction','speed','trip_id']

  # The second schema required for part B, "Trip". Note that columns 1, 4, and 5 are incomplete,
  # as mentioned in the assignment description
  Trip = pd.concat([
      pd.concat([df[['EVENT_NO_TRIP','EVENT_NO_STOP','VEHICLE_ID']], weekday,], axis=1),
      df['DIRECTION']*0
    ], axis=1)
  Trip.columns = ['trip_id','route_id','vehicle_id','service_key','direction']

  return BreadCrumb, Trip

def getNewData():
  return {
        'EVENT_NO_TRIP': [],
        'EVENT_NO_STOP': [],
        'OPD_DATE': [],
        'VEHICLE_ID': [],
        'METERS': [],
        'ACT_TIME': [],
        'VELOCITY': [],
        'DIRECTION': [],
        'RADIO_QUALITY': [],
        'GPS_LONGITUDE': [],
        'GPS_LATITUDE': [],
        'GPS_SATELLITES': [],
        'GPS_HDOP': [],
        'SCHEDULE_DEVIATION': []
  }

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    data = getNewData()

    try:
        while True:
            msg = consumer.poll(10)
            if (msg is None and total_count > 0):
                # Leave group and validate+reshape data
                logging.debug(f"processing #{total_count} breadcrumbs...")
                df = pd.DataFrame.from_dict(data)
                df = fix_types(df)
                df = validate(df)
                BreadCrumb, Trip = reshape(df)
                load_data(BreadCrumb, "BreadCrumb")
                load_data(Trip, "Trip")
                logging.debug(f"loaded data!")
                data = getNewData()
                total_count = 0
            elif msg is None:
                logging.debug(f"No breadcrumbs. Waiting a little...")
                continue
            elif msg.error():
                logging.error(msg.error())
            else:
                # Check for Kafka message, add to dataframe
                try:
                    record_value = json.loads(msg.value())
                    for key, value in data.items():
                      value.append(record_value.get(key))
                    total_count += 1
                    if (total_count % 10000 == 0):
                      logging.debug("added 10000 records to df.")
                except Exception as e:
                    logging.error(f"Failed to add breadcrumb record. {e}")
                    logging.error(f"ERROR MSG: {msg.value()}")
                    continue
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(f"UNEXPECTED ERROR: {e}")
    finally:
        logging.error("Exiting service function. Should not happen!")
        consumer.close()
