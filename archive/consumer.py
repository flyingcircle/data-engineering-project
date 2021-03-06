#! /home/production/bus-py-data/bin/python
import sys
from confluent_kafka import Consumer
from datetime import date
import json
import ccloud_lib


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
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()), file=sys.stdout)
            else:
                # Check for Kafka message
                record_value = str(msg.value(), 'utf-8')
                print(record_value, file=sys.stdout)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
