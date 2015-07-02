# This script checks Kafka topic details. The topic name is passed as the command line argument.
# e.g.- ./check_topic.sh topic-1
# The above statement would give details like which node is the leader for the topic, the number of partitions 
# and the replication factor.

#!/usr/bin/env bash

/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic $1
