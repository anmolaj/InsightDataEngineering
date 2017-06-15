from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

##For Cleaning purposes
import nltk
import string

def process(text,lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()):
	"""
	Motivation : Process the incoming tweets to be able to remove extra symbols and make them all similar
	"""

	text=text.lower()
	text=text.replace('\'s','')
	text=text.replace('\'','')  
	
	tokens=[]
	for punct in string.punctuation:
		text=text.replace(punct,' ')   
	
	
	for token in nltk.word_tokenize(text):
		try:
			tokens.append(lemmatizer.lemmatize(token).encode())
		except:
			continue
	return tokens

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
	lines =ioStream.map(lambda x:(x[0],x[1]))
	lines2 =ioStream.map(lambda x:(x[0],x[1]))
	lines.pprint()
	lines2.pprint()
	#ioStream.pprint()

	
	#lines.pprint()


	ssc.start()
	ssc.awaitTermination()
    


