from __future__ import print_function

import pickle
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.context import Row,SQLContext
import requests


#Reference :https://www.toptal.com/apache/apache-spark-streaming-twitter
def get_sql_context_instance(spark_context):
		if ('sqlContextSingletonInstance' not in globals()): globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
		return globals()['sqlContextSingletonInstance']


def convert_to_word_df(rdd):
	"""
	Converting Each Category Word Counts to dataframe from sorting and slicing purpose and sending to UI
	input: RDD conatining category word and its occurence
	output : top 10 words for each category
	"""
	
	try:
	
		
		sql_context = get_sql_context_instance(rdd.context)
		
		row_rdd=rdd.map(lambda (x,y):Row(str(x[0]), str(x[1]), y))
		
		# create a DF from the Row RDD
		tweet_df = sql_context.createDataFrame(row_rdd,['tweet_category',"tweet_word",'tweet_word_count'])

		query_words= "SELECT T.tweet_category,T.tweet_word,T.tweet_word_count FROM (SELECT tweet_category,tweet_word,tweet_word_count, RANK() OVER(PARTITION by tweet_category ORDER BY tweet_word_count DESC) AS rn FROM TweetsCategoryWords) AS T WHERE T.rn <= 10"

		tweet_df.registerTempTable("TweetsCategoryWords")

		tweets_cat_final_df = sql_context.sql(query_words)

		
		tweets_cat_final_df.show()
		send_df_to_dashboard(tweets_cat_final_df)
		
	
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)

def send_df_to_dashboard(df):
	"""
	Sending each dataframe to UI
	input: word by dataframe to display
	output: sending dataframe as appropriate inputs to POST method
	"""
	
	cat = [str(t.tweet_category) for t in df.select("tweet_category").collect()]

	cat_word=[str(t.tweet_word) for t in df.select("tweet_word").collect()]

	# extract the counts from dataframe and convert them into array
	word_count = [p.tweet_word_count for p in df.select("tweet_word_count").collect()]
	
	url = '<url>/updateWordData'
	request_data = {'labels_word': str(cat), 'word_data': str(cat_word),'word_count': str(word_count)}
	try:
		response = requests.post(url, data=request_data)
	except:
		pass

def convert_to_cat_df(rdd_cat):
	"""
	Converting Each Category Counts to dataframe and sending to UI
	input: RDD conatining category and its occurence
	output : Category with its count
	"""
	
	try:
	
		
		sql_cat_context = get_sql_context_instance(rdd_cat.context)
		row_cat_rdd=rdd_cat.map(lambda (x,y):Row(str(x), y))

		
		# create a DF from the Row RDD
		tweet_cat_df = sql_cat_context.createDataFrame(row_cat_rdd,['tweet_category',"count_cat"])

		query="Select * from TweetsCategory"

		tweet_cat_df.registerTempTable("TweetsCategory")

		tweets_cat_final_df = sql_cat_context.sql(query)
		tweets_cat_final_df.show()


		send_df_cat_to_dashboard(tweets_cat_final_df)
		
	
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)


def send_df_cat_to_dashboard(df):
	"""
	Sending each dataframe to UI
	input: category dataframe to display
	output: sending dataframe as appropriate inputs to POST method
	"""
	
	top_cat = [str(t.tweet_category) for t in df.select("tweet_category").collect()]
	# extract the counts from dataframe and convert them into array
	cat_count = [p.count_cat for p in df.select("count_cat").collect()]
	print (top_cat)
	print (cat_count)
	url = '<url>/updateData'
	request_data = {'labels': str(top_cat), 'data': str(cat_count)}
	try:
		response = requests.post(url, data=request_data)
	except:
		pass



if __name__ == "__main__":

	from TextClassification import ClassifyText

	#Windowing Reference :https://prateekvjoshi.com/2015/12/29/performing-windowed-computations-on-streaming-data-using-spark-in-python/

	batch_interval = 5
	window_length = 5 * batch_interval
	frequency = 2 * batch_interval
	checkpointInterval=6*frequency

	sc = SparkContext(appName="TweetProcess",pyFiles=['TextClassification.py'])
	ssc= StreamingContext(sc,batch_interval)
	sqlContext = SQLContext(sc)
	sc.setLogLevel("WARN")
	
	ssc.checkpoint("s3n://sparkcheckpoint/checkpoint")
	

	print ("Start---------------")
	tweet_model= ClassifyText()

	brokers = '<broker-list (in comma separated string)>'
	topic="tweet-topic"


	ioStream=KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list": brokers})
	

	lines =ioStream.map(lambda (x,y):(tweet_model.predict(tweet_model.process(y)),tweet_model.process(y)))
	lines_words=lines.flatMapValues(lambda x: x).map(lambda (x,y):((x,y),1))
	lines_category=lines.map(lambda (x,y):(x,1))
	

	count_this_batch = ioStream.count().map(lambda x:('Tweets this batch: %s' % x))

	counted_words=lines_words.reduceByKeyAndWindow(lambda c1,c2:c1+c2,lambda c1,c2:c1-c2,window_length,frequency)

	counted_category=lines_category.reduceByKeyAndWindow(lambda c1,c2:c1+c2,lambda c1,c2:c1-c2,window_length,frequency)

	counted_words.checkpoint(checkpointInterval)
	counted_category.checkpoint(checkpointInterval)
	
	
	counted_words.foreachRDD(convert_to_word_df)

	counted_category.foreachRDD(convert_to_cat_df)

	ssc.start()
	ssc.awaitTermination()
    


