#! /home/production/bus-py-data/bin/python
from confluent_kafka import Consumer
from datetime import date
import logging
import pandas as pd
import numpy as np
import json
import ccloud_lib

# from load_data_to_postgres import load_data
from reshape_data_stops import getNewData
from validate_stops import fix_types, validate

logging.basicConfig(filename='/home/production/stop_consumer.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

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
                logging.info(f"processing #{total_count} breadcrumbs...")
                df = pd.DataFrame.from_dict(data)
                df = fix_types(df)
                df = validate(df)
                res = df.to_json(orient="records")
                parsed = json.loads(res)
                out_file = open(str("../data/" + date.today())+"-stopeventoutput.json", "w")
                json.dump(parsed, out_file)
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
                      logging.info("added 10000 records to df.")
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
