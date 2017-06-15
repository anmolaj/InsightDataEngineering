from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

	try:
		del sc
	except:
		pass
	sc = SparkContext(appName="TweetProcess")
	ssc= StreamingContext(sc,5)
	sc.setLogLevel("WARN")

	brokers = 'ip-10-0-0-4:9092,ip-10-0-0-5:9092'
	topic="tweet-topic2"

	ioStream=KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list": brokers})

	#Trial code from https://apache.googlesource.com/spark/+/master/examples/src/main/python/streaming/direct_kafka_wordcount.py
	ioStream.pprint()
	# lines = ioStream.map(lambda x: x[1])
	# lines.pprint().encode('ascii', 'ignore')

	#lines.saveAsTextFiles("test1")
	# counts = ioStream.flatMap(lambda line: line.split(" ")) \
 #        .map(lambda word: (word, 1)) \
 #        .reduceByKey(lambda a, b: a+b)
 #        counts.pprint()

	ssc.start()
	ssc.awaitTermination()
    


