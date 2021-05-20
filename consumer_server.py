from kafka import KafkaConsumer

my_consume = KafkaConsumer(bootstrap_servers='localhost:9092',consumer_timeout_ms=1000,auto_offset_reset='earliest')

my_consume.subscribe(["police.dept.service.log"])

for data in my_consume:
    print(data)
