# Kafka-Streaming
# Below 2 commands is to set up zookeeper and kafka server
/usr/bin/zookeeper-server-start zookeeper.properties
/usr/bin/kafka-server-start producer.properties

#Python Kafka server
python kafka_server.py

#Kafa Consumer Command
/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic "police.dept.service.log" --from-beginning

#Spark Streaming Command
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py

#To get the UI press the PreView Option
