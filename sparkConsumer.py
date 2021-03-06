from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
	consume()

	sc = SparkContext(appName="TweetProcessing")
	ssc= SparkContext(sc,1)
	brokers=['localhost:9092']
	topic='my-topic'#"tweet-topic"

	ioStream=KafkaUtils(ssc,[topic],{"metadata.broker.list": brokers})

	print (type(ioStream))

	#Trial code from https://apache.googlesource.com/spark/+/master/examples/src/main/python/streaming/direct_kafka_wordcount.py
	lines = ioStream.map(lambda x: x[1])
	counts = ioStream.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    	counts.pprint()

	ssc.start()
    	ssc.awaitTermination()



