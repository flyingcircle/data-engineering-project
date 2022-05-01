from confluent_kafka import Consumer
from datetime import date, timedelta
import pandas as pd
import json
import sys
import ccloud_lib

# Reformat the date 
def formatDate(date):
  datemap = {
      'JAN': '1',
      'FEB': '2',
      'MAR': '3',
      'APR': '4',
      'MAY': '5',
      'JUN': '6',
      'JUL': '7',
      'AUG': '8',
      'SEP': '9',
      'OCT': '10',
      'NOV': '11',
      'DEC': '12',
  }

  d = date.split('-')
  ts = datemap[d[1]] + '-' + d[0] + '-' + d[2] + ' '

  return ts

def validate(raw):
  error_log = open('error_log_'+str(date.today()),'w')
  # Assertion 1 (existence): All records have a lat/lon
  if (raw['GPS_LATITUDE'] == '').any() or (raw['GPS_LONGITUDE'] == '').any() :
    error_log.write('Assertion 1 failed: some records are missing lat/lon values')
    raw = raw[raw['GPS_LATITUDE'] != np.nan]
    raw = raw[raw['GPS_LONGITUDE'] != np.nan]
  
  # Assertion 2 (inter-record): If bus has speed, it has direction
  if not (raw['DIRECTION'].all() == raw['VELOCITY'].all()):
    error_log.write('Assertion 2 failed: some records have speed but not direction/direction but not speed')
    raw = raw[(raw['VELOCITY'] == np.nan) == (raw['DIRECTION'] == np.nan)]

  # Assertion 3 (limit): bus velocity must be beneath 80mph
  if not (pd.to_numeric(raw['VELOCITY'])*2.236936 < 80).all():
    error_log.write('Assertion 3 failed: velocity exceeded 80mph')
    raw = raw[pd.to_numeric(raw['VELOCITY'])*2.236936 < 80]

  # Assertion 4 (summary): Meter count should always be ascending for a trip
  trips = raw.groupby('EVENT_NO_TRIP')
  test = pd.DataFrame(columns=['Trips'])
  for trip, group in trips:
    if (not pd.to_numeric(group['METERS']).is_monotonic_increasing):
      error_log.write('Assertion 4 failed: meters werent ascending')
      raw = raw[raw['EVENT_NO_TRIP' != trip]]
                
  # Assertion 5 (limit): Bus direction should be between 0 and 359
  if (pd.to_numeric(raw['DIRECTION']) < 0).any() or (pd.to_numeric(raw['DIRECTION']) > 359).any():
    error_log.write('Assertion 5 failed: bus direction was out of bounds')
    raw = raw[raw['DIRECTION'] > 0]
    raw = raw[raw['DIRECTION'] < 360]
    
  error_log.close()

  return raw

def reshape(json_data):
  json_data['VELOCITY'] = pd.to_numeric(json_data['VELOCITY'])
  json_data['ACT_TIME'] = pd.to_numeric(json_data['ACT_TIME'])
  json_data['VELOCITY'] = pd.to_numeric(json_data['VELOCITY'])
  json_data['GPS_LATITUDE'] = pd.to_numeric(json_data['GPS_LATITUDE'])
  json_data['GPS_LONGITUDE'] = pd.to_numeric(json_data['GPS_LONGITUDE'])
  json_data['DIRECTION'] = pd.to_numeric(json_data['DIRECTION'])
  json_data['EVENT_NO_TRIP'] = pd.to_numeric(json_data['EVENT_NO_TRIP'])
  json_data['EVENT_NO_STOP'] = pd.to_numeric(json_data['EVENT_NO_STOP'])
  json_data['VEHICLE_ID'] = pd.to_numeric(json_data['VEHICLE_ID'])

  json_data['VELOCITY'].replace(to_replace='', value='0')
  json_data['VELOCITY'] = pd.to_numeric(json_data['VELOCITY']).apply(lambda x: x*2.236936)

  json_data['ACT_TIME'] = json_data['ACT_TIME'].apply(lambda x: str(timedelta(seconds=x)))
  json_data['OPD_DATE'] = json_data['OPD_DATE'].apply(lambda x: formatDate(x))
  timestamp = json_data['OPD_DATE'] + json_data['ACT_TIME']

  # The first schema required for part B, "BreadCrumb"
  BreadCrumb = json_data[[
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
                          pd.concat([json_data[['EVENT_NO_TRIP','EVENT_NO_STOP','VEHICLE_ID']], timestamp,], axis=1),
                          json_data['DIRECTION']
                        ], axis=1)
  Trip.columns = ['trip_id','route_id','vehicle_id','service_key','direction']
  BreadCrumb.to_json('Breadcrumb.json')
  Trip.to_json('Trip.json')

  return BreadCrumb, Trip

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
    raw = pd.DataFrame(columns=[
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

    try:
        while True:
            msg = consumer.poll(10)
            if msg is None:
                break
            elif msg.error():
                print('error: {}'.format(msg.error()), file=sys.stdout)
            else:
                # Check for Kafka message, add to dataframe
                record_value = json.loads(msg.value())
                raw.loc[total_count] = [
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
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and validate+reshape data
        raw = validate(raw)
        BreadCrumb, Trip = reshape(raw)
        print(BreadCrumb)
        print(Trip)
        consumer.close()