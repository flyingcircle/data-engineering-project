# /home/production/data-engineering-project/stop_consumer_service.sh
#
source activate /home/production/bus-py-data/bin/activate && /home/production/data-engineering-project/stop_consumer.py -f /home/production/.confluent/librdkafka_stop.config -t stop_event
