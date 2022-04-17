# /home/production/consumer_service.sh
#
/home/production/consumer.py -f /home/production/.confluent/librdkafka.config -t breadcrumb_data &>> /home/production/logs/consumer_service.txt
