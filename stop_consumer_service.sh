# /home/production/data-engineering-project/stop_consumer_service.sh
#
source /home/production/bus-py-data/bin/activate && python3 /home/production/data-engineering-project/stop_consumer.py -f /home/production/.confluent/librdkafka_stop.config -t stop_event
