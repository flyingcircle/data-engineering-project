#! /home/production/bus-py-data/bin/python
from confluent_kafka import Consumer
from datetime import date
from dateutil import parser
import pandas as pd
import numpy as np
import json
import sys
import ccloud_lib

from load_data_to_postgres import load_data

oldest_date = parser.parse("2020-01-01 00:00:00+00:00")

def validate(df: pd.DataFrame):
  error_log = open('error_log_'+str(date.today()),'w')
  # Assertion 1 (existence): All records have a lat/lon
  if (df['GPS_LATITUDE'] == '').any() or (df['GPS_LONGITUDE'] == '').any() :
    error_log.write('Assertion 1 failed: some records are missing lat/lon values')
    df = df[df['GPS_LATITUDE'] != np.nan]
    df = df[df['GPS_LONGITUDE'] != np.nan]
  
  # Assertion 2 (inter-record): If bus has speed, it has direction
  if not (df['DIRECTION'].all() == df['VELOCITY'].all()):
    error_log.write('Assertion 2 failed: some records have speed but not direction/direction but not speed')
    df = df[(df['VELOCITY'] == np.nan) == (df['DIRECTION'] == np.nan)]

  # Assertion 3 (limit): bus velocity must be beneath 80mph
  if not (df['VELOCITY'] < 80).all():
    error_log.write('Assertion 3 failed: velocity exceeded 80mph')
    df = df[df['VELOCITY'] < 80]

  # Assertion 4 (summary): Meter count should always be ascending for a trip
  trips = df.groupby('EVENT_NO_TRIP')
  for trip, group in trips:
    if (not group['METERS'].is_monotonic_increasing):
      error_log.write("Assertion 4 failed: meters weren't ascending")
      df = df[df['EVENT_NO_TRIP' != trip]]
                
  # Assertion 5 (limit): Bus direction should be between 0 and 359
  if (df['DIRECTION'] < 0).any() or (df['DIRECTION'] > 359).any():
    error_log.write('Assertion 5 failed: bus direction was out of bounds')
    df = df[df['DIRECTION'] > 0]
    df = df[df['DIRECTION'] < 360]
    
  # Assertion 6 (existence): No records should have repetition
  if (df.duplicated().any()):
    error_log.write('Assertion 6 failed: some records are repeated')
    #if any records are repeated drop the duplicate values keeping the top
    df = df.drop_duplicates()
    
  # Assertion 7 (statistical): Satellite count for each trip is greater than 8 in average
  stat = df.groupby('EVENT_NO_TRIP').sum()/df.groupby('EVENT_NO_TRIP').count()
  if not (stat['GPS_SATELLITES'] > 8).all():
    error_log.write('Assertion 7 failed: satellite count for each trip is less than 8 in average')
  
  # Assertion 8 (summary): The number of meters a vehicle travel must not exceed some reasonable limit (300,000?)
  min_meter_value = df.groupby('EVENT_NO_TRIP').min()
  max_meter_value = df.groupby('EVENT_NO_TRIP').max()
  diff_value = max_meter_value['METERS'] - min_meter_value['METERS']
  if (diff_value.max() > 300000):
    error_log.write('Assertion 8 failed: some vehicles exceeds the expected value of 300000 meters of travel')

  # Assertion 9 (intra-record): Single trip index should have single vehicle ID 
  trip_vehicle = df.groupby('EVENT_NO_TRIP').VEHICLE_ID.unique()
  trip_vehicle_count = trip_vehicle.groupby(['EVENT_NO_TRIP']).count()
  if (trip_vehicle_count.max() > 1):
    error_log.write('Assertion 9 failed: for a single trip more than one associated vehicle_id found')
    
  # Assertion 10 (limit): ACT_TIME should be > 0
  if not (df['ACT_TIME'].min() > oldest_date):
    error_log.write('Assertion 10 failed: clock time is invalid')
    df = df[df['ACT_TIME'] > oldest_date]
    
  error_log.close()

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

def getNewDf():
  return pd.DataFrame(columns=[
        'EVENT_NO_TRIP',
        'EVENT_NO_STOP',
        'OPD_DATE',
        'VEHICLE_ID',
        'METERS',
        'ACT_TIME',
        'VELOCITY',
        'DIRECTION',
        'RADIO_QUALITY',
        'GPS_LONGITUDE',
        'GPS_LATITUDE',
        'GPS_SATELLITES',
        'GPS_HDOP',
        'SCHEDULE_DEVIATION'
    ])

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
    df = getNewDf()

    try:
        while True:
            msg = consumer.poll(10)
            if msg is None and len(df.index) > 0:
                # Leave group and validate+reshape data
                df = fix_types(df)
                df = validate(df)
                BreadCrumb, Trip = reshape(df)
                load_data(BreadCrumb, "BreadCrumb")
                load_data(Trip, "Trip")
                df = getNewDf()
            elif msg is None:
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()), file=sys.stdout)
            else:
                # Check for Kafka message, add to dataframe
                try:
                    record_value = json.loads(msg.value())
                    df.loc[total_count] = [
                      record_value['EVENT_NO_TRIP'],
                      record_value['EVENT_NO_STOP'],
                      record_value['OPD_DATE'],
                      record_value['VEHICLE_ID'],
                      record_value['METERS'],
                      record_value['ACT_TIME'],
                      record_value['VELOCITY'],
                      record_value['DIRECTION'],
                      record_value['RADIO_QUALITY'],
                      record_value['GPS_LONGITUDE'],
                      record_value['GPS_LATITUDE'],
                      record_value['GPS_SATELLITES'],
                      record_value['GPS_HDOP'],
                      record_value['SCHEDULE_DEVIATION']
                    ]
                    total_count += 1
                except:
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
