# This script creates a new Kafka topic. Replication factor, number of partitions and topic name 
# can be passed as the command line arguments
# e.g.: ./create_topic.sh 3 1 topic-1
# This would create the topic topic-1 with replication factor 3 and 1 partition

#!/usr/bin/env bash

/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor $1 --partitions $2 --topic $3

