# Kafka Cheatsheet

# Install with Homebrew
 
```
brew install zookeeper
brew install kafka
```

Start as service:

```
brew services start zookeeper
brew services start kafka
```

start as process:

```
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
kafka-server-start /usr/local/etc/kafka/server.properties
```

Create & describe topic:

```
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic test
kafka-topics --bootstrap-server localhost:9092 --describe --topic test  
```

Listen & Consume:

```
kafka-console-producer --bootstrap-server localhost:9092 --topic test 
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning 
```

# Not being able to start Kafka with Zookeeper: kafka.common.InconsistentClusterIdException
 > Reason is Kafka saved failed cluster ID in meta.properties.
 Try to delete kafka-logs/meta.properties from your tmp folder, which is located in C:/tmp folder by default on windows, and /tmp/kafka-logs on Linux

For mac, the following steps are needed.
1. Stop kafka service: brew services stop kafka
2. open kafka server.properties file: vim /usr/local/etc/kafka/server.properties find value of log.dirs in this file. For me, it is /usr/local/var/lib/kafka-logs
3. delete path-to-log.dirs/meta.properties file
4. start kafka service brew services start kafka

# Config recommendations

- Partitions: Many users will have the partition count for a topic be equal to, or a multiple of, the number of brokers in the cluster. This allows the partitions to be evenly distributed to the brokers, which will evenly distribute the message load. This is not a requirement, however, as you can also balance message load by having multiple topics.
 >  If you have some estimate regarding the target throughput of the topic and the expected throughput of the consumers, you can divide the target throughput by the expected consumer throughput and derive the number of partitions this way. So if I want to be able to write and read 1 GB/sec from a topic, and I know each consumer can only process 50 MB/s, then I know I need at least 20 partitions. This way, I can have 20 consumers reading from the topic and achieve 1 GB/sec.
 > If you don’t have this detailed information, our experience suggests that limiting the size of the partition on the disk to less than 6 GB per day of retention often gives sat‐ isfactory results. Starting small and expanding as needed is easier than starting too large.
 

