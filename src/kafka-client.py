from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from json import loads
import os

producer = KafkaProducer(bootstrap_servers='152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092')

f = open("test.txt")

for line in f:
	wordlist = line.split(" ")
	for word in wordlist:
		producer.send('words',key = b'test.txt',value=word.encode())

producer.flush()
producer.close()

consumer  = KafkaConsumer(bootstrap_servers='152.46.18.86:9092,152.46.18.71:9092,152.46.17.184:9092',group_id='dic',auto_offset_reset='earliest',enable_auto_commit=True)
topic = TopicPartition('output',0)
consumer.assign([topic])

#consumer.seek_to_end(topic)
#lastOffset = consumer.position(topic)

#consumer.seek_to_beginning(topic)

for msg in consumer:
		print(msg.key+" "+msg.value)
		#if msg.offset == lastOffset - 1:
		#	break

consumer.close()

#os.system("/opt/kafka/kafka_2.12-1.0.1/bin/kafka-topics.sh --zookeeper \"152.46.18.86:2181,152.46.18.71:2181,152.46.17.184:2181\" --delete --topic \"words\" > /dev/null 2>&1")
