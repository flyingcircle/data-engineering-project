#! /home/production/bus-py-data/bin/python
from datetime import date
import json
import ccloud_lib
import logging
import requests
import argparse
import time
from confluent_kafka import Producer, KafkaError

from trip_fetcher import get_data

if __name__ == '__main__':
    logging.basicConfig(filename='/home/production/logs/stop_producer.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    datafile = args.datafile
    conf = ccloud_lib.read_ccloud_config(config_file)
      # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    ccloud_lib.create_topic(conf, topic)
    delivered_records = 0

    if args.datafile:
        filename = args.datafile
        fp = open(filename)
        json_data = json.load(fp)
    else:
        json_data = get_data()

        # Optional per-message on_delivery handler (triggered by poll() or flush())
        # when a message has been successfully delivered or
        # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
		successful or failed delivery of message
		"""
        if err is not None:
            logging.info("Failed to deliver message: {}".format(err))
        else:
            print("record " + str(delivered_records) + " sent")
            delivered_records += 1
            logging.info(f"processing #{delivered_records} breadcrumbs...")

    record_key = str(date.today())

    for datapoint in json_data:
        record_value = json.dumps(datapoint)
        producer.produce(topic, key=record_key,
                            value=record_value, on_delivery=acked)
        time.sleep(0.0001)
        producer.poll(0)

    producer.flush()

    logging.info("{} messages were produced to topic {}!".format(delivered_records, topic))
