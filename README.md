 Kafka-Streaming
 Below 2 commands is to set up zookeeper and kafka server
/usr/bin/zookeeper-server-start zookeeper.properties
/usr/bin/kafka-server-start producer.properties

#Python Kafka server
python kafka_server.py

#Kafa Consumer Command
/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic "police.dept.service.log" --from-beginning

#Spark Streaming Command
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py

#To get the UI press the PreView Option


 Answers:
 
 1. Latency plays an important role. it effects the performance. and also, th processedRowsPerSecond.

2. ideally, we need to increase the parallel processing of the data chinks in spark so that the more data chunks(Partitions)  gets processed at a given time.to increase parallelism:
 a. increase the number of Cores of the  each Node in the cluster
 b. changing the number of partitions will also effect the performance
